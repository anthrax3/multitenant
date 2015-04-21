//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Workers.Surveys.Commands
{
    using System.Collections.Generic;
    using Tailspin.Web.Survey.Shared.Models;
    using Tailspin.Web.Survey.Shared.QueueMessages;

    public class TenantSurveyProcessingInfo
    {
        public TenantSurveyProcessingInfo(string tenant, string slugName)
        {
            this.AnswersSummary = new SurveyAnswersSummary(tenant, slugName);
            this.AnswersMessages = new List<SurveyAnswerStoredMessage>();
        }

        public SurveyAnswersSummary AnswersSummary { get; private set; }

        public IList<SurveyAnswerStoredMessage> AnswersMessages { get; private set; }
    }
}
