//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Models
{
    public class TenantUdfPageViewData<T> : TenantPageViewData<T> 
    {
        public TenantUdfPageViewData(string model, T contentModel) : base(contentModel)
        {
            this.ModelName = model;
        }

        public string ModelName { get; set; }
    }
}