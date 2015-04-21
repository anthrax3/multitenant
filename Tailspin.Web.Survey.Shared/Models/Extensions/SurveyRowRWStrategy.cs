//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Models.Extensions
{
    using System;
    using System.Data.Services.Client;
    using Microsoft.WindowsAzure.Storage;
    using Tailspin.Web.Survey.Shared.DataExtensibility;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;
    using Microsoft.WindowsAzure.Storage.Table.DataServices;

    public class SurveyRowRWStrategy : UDFModelRWStrategy<SurveyRow>
    {
        protected readonly IUDFDictionary UDFDictionary;

        public SurveyRowRWStrategy(IUDFDictionary udfDictionary)
            : base()
        {
            this.UDFDictionary = udfDictionary;
        }

        public override void ReadEntity(TableServiceContext context, ReadingWritingEntityEventArgs args)
        {
            var surveyRow = args.Entity as SurveyRow;
            if (surveyRow == null)
            {
                throw new InvalidOperationException("Strategy should be used to read/write a SurveyRow model.");
            }

            surveyRow.UserDefinedFields = this.UDFDictionary.InstanceFieldsFor<SurveyRow>(surveyRow.PartitionKey);

            base.ReadEntity(context, args);
        }
    }
}