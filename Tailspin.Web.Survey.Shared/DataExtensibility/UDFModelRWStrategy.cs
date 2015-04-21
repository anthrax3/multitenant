//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.DataExtensibility
{
    using System;
    using System.Data.Services.Client;
    using System.Linq;
    using System.Xml.Linq;
    using Microsoft.WindowsAzure.Storage;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;
    using Microsoft.WindowsAzure.Storage.Table.DataServices;

    public class UDFModelRWStrategy<T> : IAzureTableRWStrategy where T : IUDFModel
    {
        private const string DATASERVICESNS = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        private const string DATASERVICESMETADATANS = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";

        public UDFModelRWStrategy() { }

        public virtual void ReadEntity(TableServiceContext context, ReadingWritingEntityEventArgs args)
        {
            var ns = XNamespace.Get(DATASERVICESNS);
            var survey = args.Entity as IUDFModel;
            if (survey != null && survey.UserDefinedFields != null && survey.UserDefinedFields.Count > 0)
            {
                foreach (var udfItem in survey.UserDefinedFields)
                {
                    var udfField = args.Data.Descendants(ns + udfItem.Name).FirstOrDefault();
                    if (udfField != null)
                    {
                        try
                        {
                            udfItem.SetUDFValue(udfField.Value);
                        }
                        catch (Exception ex)
                        {
                            TraceHelper.TraceWarning("Cannot map value '{0}' to field '{1}': {2}", udfField.Value, udfItem.Name, ex.Message);
                            udfItem.ClearValue();
                        }
                    }
                    else
                    {
                        udfItem.ClearValue();
                    }
                }
            }
        }

        public virtual void WriteEntity(TableServiceContext context, ReadingWritingEntityEventArgs args)
        {
            var ns = XNamespace.Get(DATASERVICESNS);
            var nsmd = XNamespace.Get(DATASERVICESMETADATANS);
            var survey = args.Entity as SurveyRow;
            if (survey != null && survey.UserDefinedFields != null && survey.UserDefinedFields.Count > 0)
            {
                var properties = args.Data.Descendants(nsmd + "properties").First();
                foreach (var udfItem in survey.UserDefinedFields)
                {
                    var udfField = new XElement(ns + udfItem.Name, udfItem.Value);
                    udfField.Add(new XAttribute(nsmd + "type", udfItem.GetEdmType()));
                    properties.Add(udfField);
                }
            }
        }
    }
}
