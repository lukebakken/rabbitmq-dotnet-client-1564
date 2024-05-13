using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace WorkerServiceRabbitMQ
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ConnectionFactory _connectionFactory;
        private readonly IConnection _conn;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;

            _connectionFactory = new ConnectionFactory
            {
                HostName = "localhost",
                // UserName = "emailSchulze",
                // Password = "schu@2023",
                // VirtualHost = "emailSchulze",
                UserName = "guest",
                Password = "guest",
                DispatchConsumersAsync = true
            };

            _conn = _connectionFactory.CreateConnection();
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await base.StartAsync(cancellationToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _conn?.Close();
            await base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var tcs = new TaskCompletionSource();
            using CancellationTokenRegistration ctr = stoppingToken.Register(() => tcs.SetCanceled());
            await ConsumeQueue(_conn, "FilaLotes", tcs);
        }

        static async Task ConsumeQueue(IConnection conn, string queueName,
            TaskCompletionSource tcs)
        {
            const string exchangeName = "EmailEnviado";

            using var channel = conn.CreateModel();

            channel.QueueDeclare(queue: queueName);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (co, ea) =>
            {
                Console.WriteLine("[INFO] message body: {0}");
                try
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine(message);

                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                    PublishMessage(conn, exchangeName, $"{queueName}Enviados", message);

                    await Task.Delay(100);

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

            };

            channel.BasicConsume(queueName, false, consumer);

            await tcs.Task;
        }

        static void PublishMessage(IConnection conn, string exchangeName, string routingKey, string message)
        {
            using var channel = conn.CreateModel();

            channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct);

            var body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: null, body: body);
        }
    }
}
