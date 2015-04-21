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
    public class UDFItemFixture
    {
        [TestMethod]
        public void SetUdfDoubleValueReturnsValue()
        {
            // Setup
            var value = 100.5;
            var udfItem = new UDFItem { Name = "name", Type = UDFType.Double, Display = "display" };

            // Act
            udfItem.SetUDFValue(value.ToString());

            // Assert
            Assert.AreEqual(value, (double)udfItem.Value);
        }
        
        [TestMethod]
        public void SetUdfTrueBooleanValueReturnsTrue()
        {
            // Setup
            var udfItem = new UDFItem { Name = "name", Type = UDFType.Bool, Display = "display", Value = false };

            // Act
            udfItem.SetUDFValue("true");

            // Assert
            Assert.IsTrue((bool)udfItem.Value);
        }

        [TestMethod]
        public void SetUdfGuidValueReturnsGuid()
        {
            // Setup
            var value = Guid.NewGuid();
            var udfItem = new UDFItem { Name = "name", Type = UDFType.Guid, Display = "display" };

            // Act
            udfItem.SetUDFValue(value.ToString());

            // Assert
            Assert.AreEqual(value, (Guid)udfItem.Value);
        }

        [TestMethod]
        public void SetUdfIntegerValueReturnsInteger()
        {
            // Setup
            var value = 7;
            var udfItem = new UDFItem { Name = "name", Type = UDFType.Integer, Display = "display" };

            // Act
            udfItem.SetUDFValue(value.ToString());

            // Assert
            Assert.AreEqual(value, (int)udfItem.Value);
        }

        [TestMethod]
        public void SetUdfLongValueReturnsLong()
        {
            // Setup
            long value = 7;
            var udfItem = new UDFItem { Name = "name", Type = UDFType.Long, Display = "display" };

            // Act
            udfItem.SetUDFValue(value.ToString());

            // Assert
            Assert.AreEqual(value, (long)udfItem.Value);
        }

        [TestMethod]
        public void SetUdfStringValueReturnsString()
        {
            // Setup
            string value = "value";
            var udfItem = new UDFItem { Name = "name", Type = UDFType.String, Display = "display" };

            // Act
            udfItem.SetUDFValue(value.ToString());

            // Assert
            Assert.AreEqual(value, udfItem.Value.ToString());
        }

        [TestMethod]
        public void SetUdfemptyValueReturnsDefault()
        {
            // Setup
            var udfItem = new UDFItem { Name = "name", Type = UDFType.String, Display = "display", Value = "value" };

            // Act
            udfItem.SetUDFValue(string.Empty);

            // Assert
            Assert.IsNull(udfItem.Value);
        }

        [TestMethod]
        public void CopyFromValuesReturnsValues()
        {
            // Setup
            var udfItem = new UDFItem { Name = "name", Type = UDFType.String, Display = "display", Value = "value", Mandatory = false };
            var newValues = new UDFMetadata { Name = "new", Type = UDFType.Guid, Display = "new", Mandatory = true };

            // Act
            udfItem.CopyFrom(newValues);

            // Assert
            Assert.AreEqual(newValues.Name, udfItem.Name);
            Assert.AreEqual(newValues.Type, udfItem.Type);
            Assert.AreEqual(newValues.Mandatory, udfItem.Mandatory);
            Assert.AreEqual(newValues.Display, udfItem.Display);
        }

        [TestMethod]
        public void CopyFromValuesWithUdfItemInputReturnsValues()
        {
            // Setup
            var udfItem = new UDFItem { Name = "name", Type = UDFType.String, Display = "display", Value = "value", Mandatory = false };
            var newValues = new UDFItem { Name = "new", Type = UDFType.Guid, Display = "new", Mandatory = true };

            // Act
            udfItem.CopyFrom(newValues);

            // Assert
            Assert.AreEqual(newValues.Name, udfItem.Name);
            Assert.AreEqual(newValues.Type, udfItem.Type);
            Assert.AreEqual(newValues.Mandatory, udfItem.Mandatory);
            Assert.AreEqual(newValues.Display, udfItem.Display);
        }

        [TestMethod]
        public void CtorCopiesValuesfromSource()
        {
            // Setup
            var source = new UDFMetadata { Name = "name", Type = UDFType.String, Display = "display", Mandatory = false };

            // Act
            var udfItem = new UDFItem(source);

            // Assert
            Assert.AreEqual(source.Name, udfItem.Name);
            Assert.AreEqual(source.Type, udfItem.Type);
            Assert.AreEqual(source.Mandatory, udfItem.Mandatory);
            Assert.AreEqual(source.Display, udfItem.Display);
        }
    }
}
