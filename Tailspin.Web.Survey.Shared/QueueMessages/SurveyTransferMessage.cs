//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.QueueMessages
{
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;

    public class SurveyTransferMessage : AzureQueueMessage
    {
        public string Tenant { get; set; }
        public string SlugName { get; set; }
    }
}