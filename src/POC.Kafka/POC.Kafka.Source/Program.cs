using Confluent.Kafka;
using Newtonsoft.Json;
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
                        var obj = new Objeto()
                        {
                            Id = count,
                            Name = "Objeto-#Id:" + count,
                            DataCriacao = DateTime.Now
                        };

                        count++;
                        var dr = await p.ProduceAsync("topico_obj", new Message<Null, string>() { Value = JsonConvert.SerializeObject(obj) });
                        Console.WriteLine($"Enviando: '{dr.Value}' para '{dr.TopicPartitionOffset}' | {count}");
                        
                        Thread.Sleep(1000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }

    public class Objeto
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime DataCriacao { get; set; }
    }
}
