using System;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace AzureServiceBus.Practice
{
    class Program
    {
        static string NamespaceConnectionString = "";

        static void Main(string[] args)
        {
            Console.WriteLine("This is a Receiver");
            switch(args[0]){
                case "R":
                    Receiver().GetAwaiter().GetResult();
                    break;
                case "S":
                    Sender().GetAwaiter().GetResult();
                    break;
                case "TR":
                    TopicReceiver().GetAwaiter().GetResult();
                    break;
                case "TS":
                    TopicSender().GetAwaiter().GetResult();
                    break;
                default:
                    break;
            }
        }
        static async Task Sender()
        {
            var queue = "myfirstqueue";
            var sender = new Microsoft.Azure.ServiceBus.Core.MessageSender(NamespaceConnectionString, queue);
            var text = Console.ReadLine();

            while (text != "q"){
                var message = new Microsoft.Azure.ServiceBus.Message(System.Text.Encoding.UTF8.GetBytes(text));
                var messageWithOffset = new Microsoft.Azure.ServiceBus.Message(System.Text.Encoding.UTF8.GetBytes(text + " with offset"));
                
                await sender.ScheduleMessageAsync(messageWithOffset, DateTimeOffset.Now.AddSeconds(3));
                await sender.SendAsync(message);
                text = Console.ReadLine();
            }

            await sender.CloseAsync();
        }

        static async Task Receiver()
        {   
            var queueName = "myfirstqueue";
            var receiver = new Microsoft.Azure.ServiceBus.Core.MessageReceiver(NamespaceConnectionString, queueName)
            {

            };
            
            receiver.RegisterMessageHandler(async (message, cancelationToken) => {
                Console.WriteLine(System.Text.Encoding.UTF8.GetString(message.Body));
                await receiver.CompleteAsync(message.SystemProperties.LockToken);
            }, new MessageHandlerOptions(async args => {}) {
                AutoComplete = false
            });

            Console.ReadKey();

            await receiver.CloseAsync();
        }

        static async Task TopicSender()
        {
            var topicName = "myfirsttopic";
            var subscriptionName = "myfirstsubscription";

            var sender = new TopicClient(NamespaceConnectionString, topicName);

            var text = Console.ReadLine();

            while (text != "q"){
                var message = new Message(Encoding.UTF8.GetBytes(text));
                message.UserProperties["Type"] = "No";
                await sender.SendAsync(message);
                text = Console.ReadLine();
            }

            await sender.CloseAsync();
        }

        static async Task TopicReceiver()
        {
            var topicName = "myfirsttopic";
            var subscriptionName = "myfirstsubscription";

            var receiver = new SubscriptionClient(NamespaceConnectionString, topicName, subscriptionName);
            (await receiver.GetRulesAsync())
                .ToList()
                .ForEach(async r => await receiver.RemoveRuleAsync(r.Name));
            await receiver.AddRuleAsync(new RuleDescription("firstRule", new SqlFilter("user.Type='No'")));
            receiver.RegisterMessageHandler(async (message, cancelationToken) => {
                Console.WriteLine($"Message: {Encoding.UTF8.GetString(message.Body)}");
                await receiver.CompleteAsync(message.SystemProperties.LockToken);
            }, new MessageHandlerOptions(async args => {}){
                AutoComplete = false
            });

            Console.ReadKey();

            await receiver.CloseAsync();
        }
    }
}
