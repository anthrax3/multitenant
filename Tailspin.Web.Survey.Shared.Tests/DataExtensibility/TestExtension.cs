//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Tests.DataExtensibility
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Tailspin.Web.Survey.Shared.DataExtensibility;

    public class TestExtension : IUDFModel
    {
        private IList<UDFItem> items;

        public TestExtension()
        {
            this.items = new List<UDFItem> { new UDFItem { DefaultValue = "defaultvalue", Name = "name", Value = "value", Display = "display" } };
        }

        IList<UDFItem> IUDFModel.UserDefinedFields
        {
            get
            {
                return this.items;
            }
            set
            {
               this.items = value;
            }
        }
    }
}
