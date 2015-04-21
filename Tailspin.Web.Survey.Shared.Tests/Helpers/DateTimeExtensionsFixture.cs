//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Tests.Helpers
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Tailspin.Web.Survey.Shared.Helpers;
    
    [TestClass]
    public class DateTimeExtensionsFixture
    {
        [TestMethod]
        public void DateReturnsGetFormatedTicks()
        {
            Assert.AreEqual("0000000000000001001", new DateTime(1001).GetFormatedTicks());
        }
    }
}