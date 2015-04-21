//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Workers.Surveys.QueueHandlers
{
    using System;
    using System.Threading.Tasks;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;
    using Tailspin.Workers.Surveys.Commands;

    public class QueueHandler<T> : GenericQueueHandler<T> where T : AzureQueueMessage
    {
        private readonly IAzureQueue<T> queue;
        private TimeSpan interval;

        protected QueueHandler(IAzureQueue<T> queue)
        {
            this.queue = queue;
            this.interval = TimeSpan.FromMilliseconds(200);
        }

        public static QueueHandler<T> For(IAzureQueue<T> queue)
        {
            if (queue == null)
            {
                throw new ArgumentNullException("queue");
            }

            return new QueueHandler<T>(queue);
        }

        public QueueHandler<T> Every(TimeSpan intervalBetweenRuns)
        {
            this.interval = intervalBetweenRuns;

            return this;
        }

        public virtual void Do(ICommand<T> command)
        {
            Task.Factory.StartNew(
                () =>
                {
                    while (true)
                    {
                        this.Cycle(command);
                    }
                },
                TaskCreationOptions.LongRunning);
        }

        protected void Cycle(ICommand<T> command)
        {
            try
            {
                GenericQueueHandler<T>.ProcessMessages(this.queue, this.queue.GetMessages(1), command.Run);

                this.Sleep(this.interval);
            }
            catch (TimeoutException ex)
            {
                TraceHelper.TraceWarning(ex.TraceInformation());
            }
            catch (Exception ex)
            {
                // no exception should get here - we don't want the handler to stop (we log it as ERROR)
                TraceHelper.TraceError(ex.TraceInformation());
            }
        }
    }
}
