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
    
    public class UDFItem : UDFMetadata
    {
        public UDFItem()
        {
        }

        public UDFItem(UDFMetadata source)
        {
            this.CopyFrom(source);
        }

        public object Value { get; set; }

        public override void CopyFrom(UDFMetadata item)
        {
            base.CopyFrom(item);

            if (item is UDFItem)
            {
                this.Value = (item as UDFItem).Value;
            }
            else
            {
                this.SetUDFValue(item.DefaultValue);
            }
        }

        public void SetUDFValue(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                this.ClearValue();
                return;
            }

            switch (this.Type)
            {
                case UDFType.Bool:
                    this.Value = Convert.ToBoolean(value);
                    break;
                case UDFType.Double:
                    this.Value = Convert.ToDouble(value);
                    break;
                case UDFType.Guid:
                    this.Value = Guid.Parse(value);
                    break;
                case UDFType.Integer:
                    this.Value = Convert.ToInt32(value);
                    break;
                case UDFType.Long:
                    this.Value = Convert.ToInt64(value);
                    break;
                default:
                    this.Value = value;
                    break;
            }
        }

        public void ClearValue()
        {
            switch (this.Type)
            {
                case UDFType.Bool:
                    this.Value = default(bool);
                    break;
                case UDFType.Double:
                case UDFType.Integer:
                case UDFType.Long:
                    this.Value = default(int);
                    break;
                case UDFType.Guid:
                    this.Value = default(Guid);
                    break;
                default:
                    this.Value = default(string);
                    break;
            }
        }

        protected override void OnTypeChanged()
        {
            base.OnTypeChanged();
            this.ClearValue();
        }
    }
}
