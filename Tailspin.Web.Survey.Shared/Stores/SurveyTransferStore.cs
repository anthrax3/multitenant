//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Stores
{
    using AzureStorage;
    using QueueMessages;

    public class SurveyTransferStore : ISurveyTransferStore
    {
        private readonly IAzureQueue<SurveyTransferMessage> surveyTransferQueue;

        public SurveyTransferStore(IAzureQueue<SurveyTransferMessage> surveyTransferQueue)
        {
            this.surveyTransferQueue = surveyTransferQueue;
        }

        public void Initialize()
        {
            this.surveyTransferQueue.EnsureExist();
        }

        public void Transfer(string tenant, string slugName)
        {
            this.surveyTransferQueue.AddMessage(new SurveyTransferMessage { Tenant = tenant, SlugName = slugName });
        }
    }
}