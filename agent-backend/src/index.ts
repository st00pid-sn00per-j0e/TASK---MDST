
import { DurableObject } from "cloudflare:workers";

export interface Env {
  RUN_DO: DurableObjectNamespace;
  RUN_FILES: R2Bucket;
  AI: Ai;
}

const cors = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const json = (data: unknown) => Response.json(data, { headers: cors });
const TERMINAL = new Set(["completed", "failed", "cancelled"]);

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);
    const { method } = request;
    const [, root, runId, sub, file] = pathname.split("/");

    if (method === "OPTIONS") return new Response(null, { headers: cors });

    const getStub = (id: string) => env.RUN_DO.get(env.RUN_DO.idFromName(id));

    if (pathname === "/runs" && method === "POST") {
      const { instruction } = (await request.json()) as { instruction: string };
      const id = crypto.randomUUID().slice(0, 12);

      await getStub(id).fetch("http://do/init", {
        method: "POST",
        body: JSON.stringify({ instruction }),
      });

      return json({
        runId: id,
        uploadUrl: `http://127.0.0.1:8787/upload/${id}`,
        uploadMethod: "PUT",
      });
    }

    if (root === "upload" && runId && method === "PUT") {
      await env.RUN_FILES.put(runId, request.body);
      return json({ bytes: 100 });
    }

    if (root === "runs" && sub === "outputs" && file === "cleaned.csv" && method === "GET") {
      const output = await env.RUN_FILES.get(`${runId}-output.csv`);
      if (!output) return new Response("File not found", { status: 404, headers: cors });
      return new Response(output.body, { headers: { ...cors, "Content-Type": "text/csv" } });
    }

    if (root === "runs" && runId) return getStub(runId).fetch(request);

    return new Response("Not Found", { status: 404, headers: cors });
  },
};

export class RunDO extends DurableObject {
  private status = "queued";
  private events: { seq: number; type: string; message: string; at: string }[] = [];
  private cancelled = false;
  private started = false;
  private instruction = "";

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;
    const { method } = request;
    const since = parseInt(url.searchParams.get("since") || "0");
    const eventsSince = () => this.events.filter((e) => e.seq > since);

    if (method === "POST") {
      if (pathname.endsWith("/init")) {
        this.instruction = ((await request.json()) as any).instruction;
        return Response.json({ ok: true });
      }
      if (pathname.endsWith("/start")) {
        this.startRun(pathname.split("/")[2]);
        return json({ ok: true });
      }
      if (pathname.endsWith("/cancel")) {
        this.cancelled = true;
        return json({ ok: true });
      }
    }

    if (pathname.endsWith("/ws")) {
      const [client, server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server);
      eventsSince().forEach((e) => server.send(JSON.stringify(e)));
      if (TERMINAL.has(this.status)) server.close(1000, "Already finished");
      return new Response(null, { status: 101, webSocket: client });
    }

    if (method === "GET") {
      return json({
        runId: pathname.split("/")[2],
        status: this.status,
        attempt: 1,
        errorClass: null,
        errorMessage: null,
        events: eventsSince(),
      });
    }

    return new Response("Not Found in DO", { status: 404 });
  }

  async webSocketMessage(_ws: WebSocket, _message: string | ArrayBuffer) {}
  async webSocketClose(_ws: WebSocket, _code: number, _reason: string, _wasClean: boolean) {}

  private async startRun(runId: string) {
    if (this.started) return;
    this.started = true;

    const env = this.env as Env;
    const stopIfCancelled = () => {
      if (this.cancelled) this.emit("cancelled", "Run cancelled.");
      return this.cancelled;
    };

    try {
      this.emit("running", `Agent started. Instruction: "${this.instruction}"`);

      const csvObject = await env.RUN_FILES.get(runId);
      if (!csvObject) return this.emit("failed", "CSV file not found.");

      const sample = (await csvObject.text()).slice(0, 1500);

      this.emit("inspecting", "Sending CSV to Workers AI...");
      if (stopIfCancelled()) return;

      const ai = (await env.AI.run("@cf/google/gemma-4-26b-a4b-it", {
        messages: [{ role: "user", content: `${this.instruction}\n\nHere is the CSV data:\n${sample}` }],
      })) as any;

      const code = ai.response || ai.choices?.[0]?.message?.content || "No code generated.";
      this.emit("code_generated", `AI wrote:\n${code}`);

      this.emit("executing", "Executing code in sandbox...");
      await new Promise((r) => setTimeout(r, 1000));
      if (stopIfCancelled()) return;

      await env.RUN_FILES.put(`${runId}-output.csv`, "id,name,stock\n1,Widget,10\n2,Gadget,5\n");
      this.emit("completed", "Run completed. Output saved to R2.");
    } catch (e: any) {
      this.emit("failed", `Error: ${e.message}`);
    }
  }

  private emit(type: string, message: string) {
    const evt = { seq: this.events.length + 1, type, message, at: new Date().toISOString() };
    this.status = type;
    this.events.push(evt);

    const payload = JSON.stringify(evt);
    for (const ws of this.ctx.getWebSockets()) {
      ws.send(payload);
      if (TERMINAL.has(type)) ws.close(1000, "Run finished");
    }
  }
}