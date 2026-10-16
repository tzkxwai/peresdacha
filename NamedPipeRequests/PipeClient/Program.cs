using System.IO.Pipes;
using System.Text.Json;

const string pipeName = "requests-demo-pipe";
string clientName = args.Length > 0 ? args[0] : $"client-{Environment.ProcessId}";
int count = args.Length > 1 && int.TryParse(args[1], out int requested) ? requested : 8;
count = Math.Max(count, 8);

using var pipe = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
Console.WriteLine($"[{clientName}] Подключаемся...");
try
{
    using var connectTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    await pipe.ConnectAsync(connectTimeout.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine($"[{clientName}] Сервер не найден (таймаут 20 секунд).");
    return;
}

using var reader = new StreamReader(pipe, leaveOpen: true);
await using var writer = new StreamWriter(pipe, leaveOpen: true) { AutoFlush = true };
var pending = new HashSet<string>();

// Отправляем подряд ВСЕ заявки, не читая ответы между отправками.
for (int i = 1; i <= count; i++)
{
    string id = $"{clientName}-{Guid.NewGuid():N}-{i}";
    pending.Add(id);
    var request = new Request(id, $"Данные заявки №{i}");
    await writer.WriteLineAsync(JsonSerializer.Serialize(request));
    Console.WriteLine($"[{clientName}] Отправлена заявка #{i}, ID={id}");
}
Console.WriteLine($"[{clientName}] Отправлены все {count} заявок. Получаем ответы...");

try
{
    while (pending.Count > 0)
    {
        string? line = await reader.ReadLineAsync();
        if (line is null)
        {
            Console.WriteLine($"[{clientName}] Канал закрыт, нет ответов: {pending.Count}.");
            break;
        }
        Response? response;
        try { response = JsonSerializer.Deserialize<Response>(line); }
        catch (JsonException) { Console.WriteLine("Некорректный ответ сервера."); continue; }
        if (response is null) continue;
        if (pending.Remove(response.Id))
            Console.WriteLine($"[{clientName}] Ответ на {response.Id}: {response.Result}; осталось {pending.Count}");
        else
            Console.WriteLine($"[{clientName}] Неизвестный/повторный ID: {response.Id}");
    }
}
catch (IOException ex)
{
    Console.WriteLine($"[{clientName}] Соединение прервано: {ex.Message}");
}
Console.WriteLine($"[{clientName}] Завершение.");

record Request(string Id, string Payload);
record Response(string Id, string Result);
