﻿//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Workers.Surveys.Tests
{
    using System;
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
   using Microsoft.WindowsAzure.Storage;
    using Moq;
    using Tailspin.Workers.Surveys.Commands;
    using Tailspin.Workers.Surveys.QueueHandlers;
    using Web.Survey.Shared.Stores.AzureStorage;
    using Microsoft.WindowsAzure.Storage.Queue;

    [TestClass]
    public class BatchProcessingQueueHandlerFixture
    {
        [TestMethod]
        public void ForCreatesHandlerForGivenQueue()
        {
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();

            var queueHandler = BatchMultipleQueueHandler.For(mockQueue.Object, 1);

            Assert.IsInstanceOfType(queueHandler, typeof(BatchMultipleQueueHandler<MessageStub>));
        }

        [TestMethod]
        public void EveryReturnsSameHandlerForGivenQueue()
        {
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();
            var queueHandler = new BatchProcessingQueueHandlerStub(mockQueue.Object);

            var returnedQueueHandler = queueHandler.Every(TimeSpan.Zero);

            Assert.AreSame(queueHandler, returnedQueueHandler);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void ForThrowsWhenQueueIsNull()
        {
            BatchMultipleQueueHandler.For(default(IAzureQueue<MessageStub>), 1);
        }

        [TestMethod]
        public void DoCallsPreRunForBatch()
        {
            var message1 = new MessageStub();
            var message2 = new MessageStub();
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();
            var queue = new Queue<IEnumerable<MessageStub>>();
            queue.Enqueue(new[] { message1, message2 });
            mockQueue.Setup(q => q.GetMessages(32)).Returns(() => queue.Count > 0 ? queue.Dequeue() : new MessageStub[] { });
            var command = new Mock<IBatchCommand<MessageStub>>();
            var queueHandler = new BatchProcessingQueueHandlerStub(mockQueue.Object);

            queueHandler.Do(command.Object);

            command.Verify(c => c.PreRun(), Times.Once());
        }

        [TestMethod]
        public void DoCallsPostRunForBatch()
        {
            var message1 = new MessageStub();
            var message2 = new MessageStub();
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();
            var queue = new Queue<IEnumerable<MessageStub>>();
            queue.Enqueue(new[] { message1, message2 });
            mockQueue.Setup(q => q.GetMessages(32)).Returns(() => queue.Count > 0 ? queue.Dequeue() : new MessageStub[] { });
            var command = new Mock<IBatchCommand<MessageStub>>();
            var queueHandler = new BatchProcessingQueueHandlerStub(mockQueue.Object);

            queueHandler.Do(command.Object);

            command.Verify(c => c.PostRun(), Times.Once());
        }

        [TestMethod]
        public void DoRunsGivenCommandForEachMessage()
        {
            var message1 = new MessageStub();
            var message2 = new MessageStub();
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();
            var queue = new Queue<IEnumerable<MessageStub>>();
            queue.Enqueue(new[] { message1, message2 });
            mockQueue.Setup(q => q.GetMessages(32)).Returns(() => queue.Count > 0 ? queue.Dequeue() : new MessageStub[] { });
            var command = new Mock<IBatchCommand<MessageStub>>();
            var queueHandler = new BatchProcessingQueueHandlerStub(mockQueue.Object);

            queueHandler.Do(command.Object);

            command.Verify(c => c.Run(It.IsAny<MessageStub>()), Times.Exactly(2));
            command.Verify(c => c.Run(message1));
            command.Verify(c => c.Run(message2));
        }

        [TestMethod]
        public void DoDeletesMessageWhenRunIsSuccessfull()
        {
            var message = new MessageStub();
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();
            var queue = new Queue<IEnumerable<MessageStub>>();
            queue.Enqueue(new[] { message });
            mockQueue.Setup(q => q.GetMessages(32)).Returns(() => queue.Count > 0 ? queue.Dequeue() : new MessageStub[] { });
            var command = new Mock<IBatchCommand<MessageStub>>();
            command.Setup(c => c.Run(It.IsAny<MessageStub>())).Returns(true);
            var queueHandler = new BatchProcessingQueueHandlerStub(mockQueue.Object);
            
            queueHandler.Do(command.Object);

            mockQueue.Verify(q => q.DeleteMessage(message));
        }

        [TestMethod]
        public void DoDeletesMessageWhenRunIsNotSuccessfullAndMessageHasBeenDequeuedMoreThanFiveTimes()
        {
            var message = new MessageStub();
            var cloudQueueStub = new CloudQueueMessage(string.Empty);
            //var msgStub = new CloudQueueMessageStub(string.Empty);
            message.SetMessageReference(cloudQueueStub);
            var mockQueue = new Mock<IAzureQueue<MessageStub>>();
            var queue = new Queue<IEnumerable<MessageStub>>();
            queue.Enqueue(new[] { message });
            mockQueue.Setup(q => q.GetMessages(32)).Returns(() => queue.Count > 0 ? queue.Dequeue() : new MessageStub[] { });
                 
            var command = new Mock<IBatchCommand<MessageStub>>();
            command.Setup(c => c.Run(It.IsAny<MessageStub>())).Throws(new Exception("This will cause the command to fail"));
            var queueHandler = new BatchProcessingQueueHandlerStub(mockQueue.Object);

            queueHandler.Do(command.Object);
            //Hieu: since DequeueCount is readonly properties, have no solution so far for dequeued more than 5 times.
            mockQueue.Verify(q => q.DeleteMessage(message));
        }

        public class MessageStub : AzureQueueMessage
        {
        }

        public class CloudQueueMessageStub 
        {
            
            public CloudQueueMessage QueueMessage{get;set;}
           
            public CloudQueueMessageStub(string content)
            {
                this.QueueMessage = new CloudQueueMessage(content);                
                //this.QueueMessage.DequeueCount = 6;
            }
        }

        private class BatchProcessingQueueHandlerStub : BatchMultipleQueueHandler<MessageStub>
        {
            public BatchProcessingQueueHandlerStub(IAzureQueue<MessageStub> queue)
                : base(queue, 32)
            {
            }

            public override void Do(IBatchCommand<MessageStub> batchCommand)
            {
                this.Cycle(batchCommand);               
            }
        }
    }
}
