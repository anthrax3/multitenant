//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Stores.AzureStorage
{
    using System.Collections.Generic;
    using Tailspin.Web.Survey.Shared.Stores.Azure;

    public interface IAzureQueue<T> : IAzureObjectWithRetryPolicyFactory where T : AzureQueueMessage
    {
        void EnsureExist();
        void Clear();
        void AddMessage(T message);
        T GetMessage();
        IEnumerable<T> GetMessages(int maxMessagesToReturn);
        void DeleteMessage(T message);
    }
}