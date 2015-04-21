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
    public class TenantPageViewData<T> : TenantMasterPageViewData
    {
        private readonly T contentModel;

        public TenantPageViewData(T contentModel)
        {
            this.contentModel = contentModel;
        }

        public T ContentModel
        {
            get
            {
                return this.contentModel;
            }
        }
    }
}