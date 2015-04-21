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
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;

    public static class BatchMultipleQueueHandler
    {
        public static BatchMultipleQueueHandler<T> For<T>(IAzureQueue<T> queue, int batchSize) where T : AzureQueueMessage
        {
            return BatchMultipleQueueHandler<T>.For(queue, batchSize);
        }
    }
}