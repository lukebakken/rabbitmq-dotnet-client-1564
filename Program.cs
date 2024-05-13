using System.Text;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var cf = new ConnectionFactory { DispatchConsumersAsync = true };
using var conn = cf.CreateConnection();
using var channel = conn.CreateModel();

var q = channel.QueueDeclare();
string queueName = q.QueueName;

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.Received += async (co, ea) =>
{
    Console.WriteLine("[INFO] message body: {0}", Encoding.UTF8.GetString(ea.Body.ToArray()));
    await Task.Delay(100);
};

channel.BasicConsume(queueName, false, consumer);

Console.WriteLine("[INFO] hit [Enter] to exit");
Console.ReadLine();
