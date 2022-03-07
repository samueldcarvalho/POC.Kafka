using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace POC.Kafka.Source
{
    class Program
    {
        static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var count = 0;
                    while (true)
                    {
                        count++;
                        var dr = await p.ProduceAsync("test-topic", new Message<Null, string> { Value = $"TESTE DE MENSAGEM: Nº{count}" });
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}' | {count}");

                        Thread.Sleep(4000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
