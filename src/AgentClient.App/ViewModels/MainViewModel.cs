

using System.Collections.ObjectModel;
using System.IO;
using AgentClient.Core.Http;
using AgentClient.Core.Models;
using AgentClient.Core.WebSockets;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Microsoft.Win32;

namespace AgentClient.App.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private readonly AgentApiClient _api;
    private readonly UploadService _upload;
    private readonly ProgressStream _stream;
    private CancellationTokenSource? _cts;

    public MainViewModel(AgentApiClient api, UploadService upload, ProgressStream stream)
    {
        _api = api;
        _upload = upload;
        _stream = stream;
    }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(StartRunCommand))]
    [NotifyCanExecuteChangedFor(nameof(CancelRunCommand))]
    private bool _isRunning;


    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(DownloadOutputCommand))]
    private bool _isCompleted;

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(StartRunCommand))]
    private string? _selectedFilePath;

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(CancelRunCommand))]
    private string? _currentRunId;

    [ObservableProperty]
    private string _status = "idle";

    [ObservableProperty]
    private string _userPrompt = "";

    [ObservableProperty]
    private double _uploadProgress;

    [ObservableProperty]
    private string? _errorClass;

    [ObservableProperty]
    private string? _errorMessage;


    public ObservableCollection<RunEvent> Events { get; } = new();

    [RelayCommand]
    private void PickFile()
    {
        var dlg = new OpenFileDialog { Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*" };
        if (dlg.ShowDialog() == true)
            SelectedFilePath = dlg.FileName;
    }

    [RelayCommand(CanExecute = nameof(CanStartRun))]
    private async Task StartRunAsync()
    {
        if (SelectedFilePath is null) return;

        Events.Clear();
        Events.Add(new RunEvent(0, "user", UserPrompt, DateTimeOffset.Now));
        ErrorClass = null;
        ErrorMessage = null;
        UploadProgress = 0;
        IsRunning = true;
        Status = "creating";
        _cts = new CancellationTokenSource();

        // Helper function to print debug messages to the UI list
        void DebugLog(string msg) => Events.Add(new RunEvent(0, "DEBUG", msg, DateTimeOffset.Now));

        try
        {
            DebugLog("1. Sending POST /runs...");
            var create = await _api.CreateRunAsync(new CreateRunRequest(UserPrompt), _cts.Token);
            
            if (create == null) {
                DebugLog("ERROR: Server returned null response.");
                return;
            }
            DebugLog($"2. Success! Run ID: {create.RunId}");
            CurrentRunId = create.RunId;

            DebugLog("3. Uploading CSV...");
            Status = "uploading";
            var size = new FileInfo(SelectedFilePath).Length;
            var progress = new Progress<long>(bytes => UploadProgress = size > 0 ? (double)bytes / size * 100 : 0);

            await _upload.UploadAsync(SelectedFilePath, create.UploadUrl, create.UploadMethod, progress, _cts.Token);
            DebugLog("4. Upload complete.");

            DebugLog("5. Sending POST /start...");
            Status = "starting";
            await _api.StartRunAsync(create.RunId, _cts.Token);
            DebugLog("6. Start complete.");

            DebugLog("7. Connecting to WebSocket (ProgressStream)...");
            Status = "running";
            
            await foreach (var evt in _stream.StreamAsync(create.RunId, 0, _cts.Token))
            {
                Events.Add(evt);
                Status = evt.Type;
            
                if (evt.Type == "failed")
                {
                    DebugLog("Caught failure event, fetching snapshot...");
                    var snap = await _api.GetRunAsync(create.RunId, 0, _cts.Token);
                    ErrorClass = snap.ErrorClass;
                    ErrorMessage = snap.ErrorMessage;
                }
            }
            DebugLog("WebSocket stream ended.");
        }
        catch (OperationCanceledException)
        {
            Status = "cancelled";
            DebugLog("Run cancelled by user.");
        }
        catch (AgentApiException ex)
        {
            ErrorClass = "api_error";
            ErrorMessage = $"{ex.StatusCode}: {ex.Body}";
            Status = "failed";
            DebugLog($"API Error: {ex.StatusCode}");
        }
        catch (Exception ex)
        {
            ErrorClass = "internal_error";
            ErrorMessage = ex.ToString(); // Full stack trace!
            Status = "failed";
            DebugLog($"CRASH: {ex.Message}");
        }
        finally
        {
            IsRunning = false;
            IsCompleted = Status == "completed";
            CurrentRunId = null;
            _cts?.Dispose();
            _cts = null;

        }
    }

    private bool CanStartRun() => !IsRunning && SelectedFilePath is not null;

    [RelayCommand(CanExecute = nameof(CanCancelRun))]
    private async Task CancelRunAsync()
    {
        if (CurrentRunId is null) return;
        try
        {
            await _api.CancelRunAsync(CurrentRunId, CancellationToken.None);
        }
        catch (Exception ex)
        {
            ErrorMessage = ex.Message;
        }
    }

        private bool CanCancelRun() => IsRunning && CurrentRunId is not null;

            [RelayCommand(CanExecute = nameof(CanDownloadOutput))]
        private async Task DownloadOutputAsync()
        {
            if (CurrentRunId is null) return;

            var dlg = new SaveFileDialog 
            { 
                FileName = $"cleaned_{CurrentRunId}.csv", 
                Filter = "CSV files (*.csv)|*.csv" 
            };

            if (dlg.ShowDialog() != true) return;

            try
            {
                var bytes = await _api.DownloadOutputAsync(CurrentRunId, "cleaned.csv", CancellationToken.None);
                await File.WriteAllBytesAsync(dlg.FileName, bytes);
                Status = "downloaded";
            }
            catch (Exception ex)
            {
                ErrorMessage = $"Download failed: {ex.Message}";
            }
        }

        private bool CanDownloadOutput() => IsCompleted && CurrentRunId is not null;
}