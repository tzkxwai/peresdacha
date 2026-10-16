using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text.Json;

const string pipeName = "requests-demo-pipe";
using var shutdown = new CancellationTokenSource();
using var slots = new SemaphoreSlim(2, 2); // Общий лимит для ВСЕХ клиентов.
var connections = new ConcurrentBag<Task>();
var currentCount = 0;
var connectionId = 0;

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // Не завершаем процесс мгновенно.
    if (!shutdown.IsCancellationRequested)
    {
        Console.WriteLine("\n[SERVER] Ctrl+C: новые подключения/заявки больше не принимаются.");
        shutdown.Cancel();
    }
};

Console.WriteLine($"[SERVER] Канал: {pipeName}; максимум 2 заявки одновременно.");
Console.WriteLine("[SERVER] Нажмите Ctrl+C для корректной остановки.");

try
{
    while (!shutdown.IsCancellationRequested)
    {
        // Отдельный экземпляр трубы на каждое подключение.
        var pipe = new NamedPipeServerStream(
            pipeName, PipeDirection.InOut, NamedPipeServerStream.MaxAllowedServerInstances,
            PipeTransmissionMode.Byte, PipeOptions.Asynchronous);

        try
        {
            await pipe.WaitForConnectionAsync(shutdown.Token);
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            await pipe.DisposeAsync();
            break;
        }
        catch
        {
            await pipe.DisposeAsync();
            throw;
        }

        int id = Interlocked.Increment(ref connectionId);
        Console.WriteLine($"[SERVER] Клиент #{id} подключился.");
        Task handler = ServeClientAsync(pipe, id);
        connections.Add(handler);
    }
}
catch (IOException ex)
{
    Console.WriteLine($"[SERVER] Ошибка канала: {ex.Message}");
}
finally
{
    Console.WriteLine("[SERVER] Ожидание завершения уже принятых заявок...");
    // Клиентские обработчики завершают свои задачи прежде, чем заканчиваются сами.
    await Task.WhenAll(connections.ToArray());
    Console.WriteLine("[SERVER] Все начатые обработки завершены. Сервер остановлен.");
}

async Task ServeClientAsync(NamedPipeServerStream pipe, int clientId)
{
    await using (pipe)
    using (var reader = new StreamReader(pipe, leaveOpen: true))
    await using (var writer = new StreamWriter(pipe, leaveOpen: true) { AutoFlush = true })
    {
        using var writeLock = new SemaphoreSlim(1, 1);
        var requests = new List<Task>();

        try
        {
            // Чтение новых сообщений не блокируется обработкой прежних.
            while (!shutdown.IsCancellationRequested && pipe.IsConnected)
            {
                string? line;
                try { line = await reader.ReadLineAsync(shutdown.Token); }
                catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { break; }
                if (line is null) break;

                Request? request;
                try { request = JsonSerializer.Deserialize<Request>(line); }
                catch (JsonException)
                {
                    Console.WriteLine($"[SERVER] Клиент #{clientId}: некорректный JSON.");
                    continue;
                }
                if (request is null || string.IsNullOrWhiteSpace(request.Id)) continue;

                // Задачи запускаются сразу, ограничение действует только на обработку.
                requests.Add(ProcessAsync(request, clientId, writer, writeLock));
            }
        }
        catch (IOException ex)
        {
            Console.WriteLine($"[SERVER] Клиент #{clientId}: соединение прервано: {ex.Message}");
        }
        finally
        {
            await Task.WhenAll(requests);
            Console.WriteLine($"[SERVER] Клиент #{clientId}: все принятые заявки завершены.");
        }
    }
}

async Task ProcessAsync(Request request, int clientId, StreamWriter writer, SemaphoreSlim writeLock)
{
    await slots.WaitAsync(); // Не отменяем принятую заявку при Ctrl+C.
    Response response;
    try
    {
        int count = Interlocked.Increment(ref currentCount);
        Console.WriteLine($"[START] Клиент #{clientId}, заявка {request.Id}, в работе: {count}");
        int delayMs = Random.Shared.Next(500, 1501);
        await Task.Delay(delayMs);
        response = new Response(request.Id, $"Успешно обработана за {delayMs} мс");
        count = Interlocked.Decrement(ref currentCount);
        Console.WriteLine($"[END]   Клиент #{clientId}, заявка {request.Id}, в работе: {count}");
    }
    finally
    {
        slots.Release();
    }

    // Синхронизируем отправку по одному каналу, чтобы JSON-строки не перемешивались.
    await writeLock.WaitAsync();
    try
    {
        await writer.WriteLineAsync(JsonSerializer.Serialize(response));
    }
    catch (IOException)
    {
        Console.WriteLine($"[SERVER] Ответ для {request.Id} не доставлен: клиент отключился.");
    }
    catch (ObjectDisposedException)
    {
        Console.WriteLine($"[SERVER] Ответ для {request.Id} не доставлен: канал закрыт.");
    }
    finally
    {
        writeLock.Release();
    }
}

record Request(string Id, string Payload);
record Response(string Id, string Result);
