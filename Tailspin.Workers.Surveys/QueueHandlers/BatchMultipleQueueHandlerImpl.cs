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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;
    using Tailspin.Workers.Surveys.Commands;
    
    public class BatchMultipleQueueHandler<T> : GenericQueueHandler<T> where T : AzureQueueMessage
    {
        private readonly IList<QueueBatchConfiguration> queuesConfiguration;
        private TimeSpan interval;
        private int maxBatchesPerCycle;

        protected BatchMultipleQueueHandler(IAzureQueue<T> queue, int batchSize)
        {
            this.queuesConfiguration = new List<QueueBatchConfiguration>();
            this.queuesConfiguration.Add(QueueBatchConfiguration.BuildConfig(queue, batchSize));
            this.interval = TimeSpan.FromMilliseconds(200);
            this.maxBatchesPerCycle = 10;
        }

        public static BatchMultipleQueueHandler<T> For(IAzureQueue<T> queue, int batchSize)
        {
            if (queue == null)
            {
                throw new ArgumentNullException("queue");
            }

            batchSize = Math.Max(1, batchSize);

            return new BatchMultipleQueueHandler<T>(queue, batchSize);
        }

        public BatchMultipleQueueHandler<T> AndFor(IAzureQueue<T> queue, int batchSize)
        {
            if (queue == null)
            {
                throw new ArgumentNullException("queue");
            }

            batchSize = Math.Max(1, batchSize);

            this.queuesConfiguration.Add(QueueBatchConfiguration.BuildConfig(queue, batchSize));

            return this;
        }

        public BatchMultipleQueueHandler<T> Every(TimeSpan intervalBetweenRuns)
        {
            this.interval = intervalBetweenRuns;

            return this;
        }

        public BatchMultipleQueueHandler<T> WithLessThanTheseBatchIterationsPerCycle(int maxBatchesPerCycle)
        {
            this.maxBatchesPerCycle = maxBatchesPerCycle;

            return this;
        }

        public virtual void Do(IBatchCommand<T> batchCommand)
        {
            Task.Factory.StartNew(
                () =>
                {
                    while (true)
                    {
                        this.Cycle(batchCommand);
                    }
                },
                TaskCreationOptions.LongRunning);
        }

        protected void Cycle(IBatchCommand<T> batchCommand)
        {
            try
            {
                batchCommand.PreRun();

                int batches = 0;
                bool continueProcessing;
                do
                {
                    continueProcessing = false;
                    foreach (var queueConfig in this.queuesConfiguration)
                    {
                        var messages = queueConfig.Queue.GetMessages(queueConfig.BatchSize);
                        GenericQueueHandler<T>.ProcessMessages(queueConfig.Queue, messages, batchCommand.Run);
                        continueProcessing |= messages.Count() >= queueConfig.BatchSize;
                    }
                    batches++;
                }
                while (continueProcessing && batches < this.maxBatchesPerCycle);

                batchCommand.PostRun();

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

        private class QueueBatchConfiguration
        {
            public IAzureQueue<T> Queue { get; set; }
            
            public int BatchSize { get; set; }

            public static QueueBatchConfiguration BuildConfig(IAzureQueue<T> queue, int batchSize)
            {
                return new QueueBatchConfiguration() { Queue = queue, BatchSize = batchSize };
            }
        }
    }
}
