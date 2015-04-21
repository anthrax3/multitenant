//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Stores.AzureSql
{
    using Microsoft.Practices.TransientFaultHandling;
    using Tailspin.Web.Survey.Shared.Stores.Azure;

    public class AzureSqlWithRetryPolicy : AzureObjectWithRetryPolicyFactory
    {
        protected RetryPolicy CommandRetryPolicy
        {
            get
            {
                var retryPolicy = this.GetRetryPolicyFactoryInstance().GetDefaultSqlCommandRetryPolicy();
                retryPolicy.Retrying += new System.EventHandler<RetryingEventArgs>(RetryPolicyTrace);
                return retryPolicy;
            }
        }

        protected RetryPolicy ConnectionRetryPolicy
        {
            get
            {
                var retryPolicy = this.GetRetryPolicyFactoryInstance().GetDefaultSqlConnectionRetryPolicy();
                retryPolicy.Retrying += new System.EventHandler<RetryingEventArgs>(RetryPolicyTrace);
                return retryPolicy;
            }
        }
    }
}
