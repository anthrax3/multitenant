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
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Tailspin.Web.Survey.Shared.Stores;
    using Web.Survey.Shared.QueueMessages;

    public class UpdatingSurveyResultsSummaryCommand : IBatchCommand<SurveyAnswerStoredMessage>
    {
        private readonly IDictionary<string, TenantSurveyProcessingInfo> tenantSurveyProcessingInfoCache;
        private readonly ISurveyAnswerStore surveyAnswerStore;
        private readonly ISurveyAnswersSummaryStore surveyAnswersSummaryStore;

        public UpdatingSurveyResultsSummaryCommand(IDictionary<string, TenantSurveyProcessingInfo> processingInfoCache, ISurveyAnswerStore surveyAnswerStore, ISurveyAnswersSummaryStore surveyAnswersSummaryStore)
        {
            this.tenantSurveyProcessingInfoCache = processingInfoCache;
            this.surveyAnswerStore = surveyAnswerStore;
            this.surveyAnswersSummaryStore = surveyAnswersSummaryStore;
        }

        public void PreRun()
        {
            this.tenantSurveyProcessingInfoCache.Clear();
        }

        public bool Run(SurveyAnswerStoredMessage message)
        {
            var surveyAnswer = this.surveyAnswerStore.GetSurveyAnswer(
                                    message.Tenant,
                                    message.SurveySlugName,
                                    message.SurveyAnswerBlobId);

            var keyInCache = string.Format(CultureInfo.InvariantCulture, "{0}-{1}", message.Tenant, message.SurveySlugName);
            TenantSurveyProcessingInfo surveyInfo;

            if (!this.tenantSurveyProcessingInfoCache.ContainsKey(keyInCache))
            {
                surveyInfo = new TenantSurveyProcessingInfo(message.Tenant, message.SurveySlugName);
                this.tenantSurveyProcessingInfoCache[keyInCache] = surveyInfo;
            }
            else
            {
                surveyInfo = this.tenantSurveyProcessingInfoCache[keyInCache];
            }

            surveyInfo.AnswersSummary.AddNewAnswer(surveyAnswer);
            surveyInfo.AnswersMessages.Add(message);

            return false;   // won't remove the message from the queue
        }

        public void PostRun()
        {
            foreach (var surveyInfo in this.tenantSurveyProcessingInfoCache.Values)
            {
                try
                {
                    // step 1. append answers to the survey answers list
                    var notAppendedMessages = surveyInfo.AnswersMessages
                        .Where(m => !m.AppendedToAnswers)                        
                        .ToList();

                    if (notAppendedMessages.Count > 0)
                    {
                        this.surveyAnswerStore.AppendSurveyAnswerIdsToAnswersList(
                            surveyInfo.AnswersSummary.Tenant,
                            surveyInfo.AnswersSummary.SlugName,
                            notAppendedMessages.Select(m => m.SurveyAnswerBlobId));

                        foreach (var message in notAppendedMessages)
                        {
                            try
                            {
                                message.AppendedToAnswers = true;
                                message.UpdateQueueMessage();
                            }
                            catch (Exception ex)
                            {
                                TraceHelper.TraceError(
                                    "Error updating message for '{0}-{1}': {2}",
                                    message.Tenant,
                                    message.SurveySlugName,
                                    ex.TraceInformation());
                            }
                        }
                    }

                    // step 2. update the summary for survey/tenant
                    this.surveyAnswersSummaryStore.MergeSurveyAnswersSummary(surveyInfo.AnswersSummary);

                    // step 3. delete the set of correctly processed messages
                    foreach (var message in surveyInfo.AnswersMessages)
                    {
                        try
                        {
                            message.DeleteQueueMessage();
                        }
                        catch (Exception ex)
                        {
                            TraceHelper.TraceError(
                                "Error deleting message for '{0}-{1}': {2}",
                                message.Tenant,
                                message.SurveySlugName,
                                ex.TraceInformation());
                        }
                    }
                }
                catch (Exception ex)
                {
                    // do nothing - will leave the messages in the queue for reprocessing
                    TraceHelper.TraceWarning(ex.TraceInformation());
                }
            }
        }
    }
}
