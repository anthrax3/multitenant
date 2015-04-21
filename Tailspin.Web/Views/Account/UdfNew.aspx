<%--
===============================================================================
 Microsoft patterns & practices
 Windows Azure Architecture Guide
===============================================================================
 Copyright © Microsoft Corporation.  All rights reserved.
 This code released under the terms of the 
 Microsoft patterns & practices license (http://wag.codeplex.com/license)
===============================================================================
--%>

<%@ Page Title="" Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Models.TenantUdfPageViewData<Tailspin.Web.Survey.Shared.DataExtensibility.UDFMetadata>>" %>

<asp:Content ID="Content2" ContentPlaceHolderID="MenuContent" runat="server">
    <ul>
        <li><%:Html.ActionLink("New Survey", "New", "Surveys", new { area = "Survey" }, null)%></li>
        <li><%:Html.ActionLink("My Surveys", "Index", "Surveys", new { area = "Survey" }, null)%></li>
        <li><%:Html.ActionLink("My Account", "Index", "Account", new { area = string.Empty }, null)%></li>
        <li class="current"><a>Model extensions</a></li>
    </ul>
    <div class="clear">
    </div>
</asp:Content>

<asp:Content ID="Content1" ContentPlaceHolderID="MainContent" runat="server">
    <div class="breadcrumbs">
        <%: Html.ActionLink("Extendable Models", "ModelIndex", "Account")%> &gt;
        <%: Html.ActionLink(this.Model.ModelName, "UdfIndex", "Account", new { model = this.Model.ModelName }, null)%> &gt;
        <%: this.Model.Title %>
    </div>
    <div id="issuerOptionTabs">
        <div class="sectionexplanationcontainer">
            <span class="titlesection">User Defined Field</span> <span class="explanationsection">
                <div id="yourIssuerTab" class="issuerOptionTab">
                    <%using (Html.BeginForm())
                      {%>
                          <%: Html.ValidationSummary(true) %>
                          <%: Html.AntiForgeryToken() %>
                    <div class="sampleform">
                        <table class="configTable">
                            <tbody>
                                <tr>
                                    <td>
                                        Field name:
                                    </td>
                                    <td>
                                        <%= Html.TextBox("metadata.Name", this.Model.ContentModel.Name)%>
                                        <%= Html.ValidationMessageFor(m => m.ContentModel.Name) %>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Field Type:
                                    </td>
                                    <td>
                                        <%= Html.DropDownList("metadata.Type",
                                            new List<SelectListItem>()
                                            {
                                                new SelectListItem() { Text = "Integer" },
                                                new SelectListItem() { Text = "Double" },
                                                new SelectListItem() { Text = "String", Selected = true }
                                            })%>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Display text:
                                    </td>
                                    <td>
                                        <%= Html.TextBox("metadata.Display",  this.Model.ContentModel.Display)%>
                                        <%= Html.ValidationMessageFor(m => m.ContentModel.Display) %>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Is Mandatory:
                                    </td>
                                    <td>
                                        <%= Html.CheckBox("metadata.Mandatory", this.Model.ContentModel.Mandatory)%>
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Default value:
                                    </td>
                                    <td>
                                        <%= Html.TextBox("metadata.DefaultValue", this.Model.ContentModel.DefaultValue)%>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div style="text-align: right; margin-top: 10px;">
                        <input type="submit" value="Add UDF" />
                    </div>
                    <% } %>
                </div>
            </span>
        </div>
    </div>
</asp:Content>

