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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Tailspin.Web.Survey.Shared.DataExtensibility;

    [TestClass]
    public class UDFMetadataFixture
    {
        [TestMethod]
        public void GetEdmReturnsBooleanType()
        {
            this.TestGetEdmType("Edm.Boolean", UDFType.Bool);
        }

        [TestMethod]
        public void GetEdmReturnsDoubleType()
        {
            this.TestGetEdmType("Edm.Double", UDFType.Double);
        }

        [TestMethod]
        public void GetEdmReturnsGuidType()
        {
            this.TestGetEdmType("Edm.Guid", UDFType.Guid);
        }

        [TestMethod]
        public void GetEdmReturnsIntegerType()
        {
            this.TestGetEdmType("Edm.Int32", UDFType.Integer);
        }

        [TestMethod]
        public void GetEdmReturnsLongType()
        {
            this.TestGetEdmType("Edm.Int64", UDFType.Long);
        }

        [TestMethod]
        public void GetEdmReturnsStringType()
        {
            this.TestGetEdmType("Edm.String", UDFType.String);
        }

        [TestMethod]
        public void SetUdfTypeReturnsBool()
        {
            this.TestSetUdfType("Edm.Boolean");
        }

        [TestMethod]
        public void SetUdfTypeBoolReturnsDouble()
        {
            this.TestSetUdfType("Edm.Double");
        }

        [TestMethod]
        public void SetUdfTypeReturnsGuid()
        {
            this.TestSetUdfType("Edm.Guid");
        }

        [TestMethod]
        public void SetUdfTypeBoolReturnsInteger()
        {
            this.TestSetUdfType("Edm.Int32");
        }

        [TestMethod]
        public void SetUdfTypeReturnsLong()
        {
            this.TestSetUdfType("Edm.Int64");
        }

        [TestMethod]
        public void SetUdfTypeReturnsString()
        {
            this.TestSetUdfType("Edm.String");
        }

        [TestMethod]
        public void CopyFromReturnsUpdatedMetadataProperties()
        {
            // Setup
            var metadata = new UDFMetadata { Type = UDFType.String, Name = "name", Display = "display", DefaultValue = "default", Mandatory = true };

            // Act
            var newMetadata = new UDFMetadata();
            newMetadata.CopyFrom(metadata);

            // Assert
            Assert.AreEqual(metadata.Type, newMetadata.Type);
            Assert.AreEqual(metadata.Name, newMetadata.Name);
            Assert.AreEqual(metadata.Display, newMetadata.Display);
            Assert.AreEqual(metadata.Mandatory, newMetadata.Mandatory);
        }

        private void TestSetUdfType(string type)
        {
            // Setup
            var metadata = new UDFMetadata();

            // Act
            metadata.SetUDFType(type);

            // Assert
            Assert.AreEqual(type, metadata.GetEdmType());
        }

        private void TestGetEdmType(string type, UDFType udfType)
        {
            // Setup
            var metadata = new UDFMetadata { Type = udfType };

            // Act
            var result = metadata.GetEdmType();

            // Assert
            Assert.AreEqual(type, result);
        }
    }
}
