using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Data.Common;
using System.Text;
using System.Threading.Channels;

namespace WorkerServiceRabbitMQ
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ConnectionFactory _connectionFactory;
        private IConnection _conn;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;

            _connectionFactory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "emailSchulze",
                Password = "schu@2023",
                VirtualHost = "emailSchulze",
                DispatchConsumersAsync = true
            };
          
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            _conn = _connectionFactory.CreateConnection();
            await base.StartAsync(cancellationToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _conn?.Close();
            await base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {              
                await ConsumeQueue(_conn, "FilaLotes");
                await Task.Delay(1000, stoppingToken);
            }
        }

        public Task ConsumeQueue(IConnection conn, string queueName)
        {
            using var channel = conn.CreateModel();

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
                    PublishMessage(conn, "EmailEnviado", $"{queueName}Enviados", message);

                    await Task.Delay(100);

                }catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

            };

            channel.BasicConsume(queueName, false, consumer);

            return Task.CompletedTask;
        }

        void PublishMessage(IConnection conn, string v1, string v2, string message)
        {
            using var channel = conn.CreateModel();
            var body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: "EmailEnviado", routingKey: "FilaLotesEnviados", basicProperties: null, body: body);
        }
    }
}
