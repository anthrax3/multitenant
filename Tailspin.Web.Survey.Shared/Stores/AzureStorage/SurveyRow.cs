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
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Tailspin.Web.Survey.Shared.DataExtensibility;
    //using Microsoft.WindowsAzure.Storage.Table.DataServices;
    using Microsoft.WindowsAzure.Storage.Table;

    public class SurveyRow : TableEntity, IUDFModel
    {
        public string SlugName { get; set; }

        public string Title { get; set; }

        public DateTime CreatedOn { get; set; }

        public IList<UDFItem> UserDefinedFields { get; set; }
    }
}