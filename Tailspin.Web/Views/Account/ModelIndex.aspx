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

<%@ Page Title="" Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Models.TenantUdfPageViewData<IEnumerable<string>>>" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MenuContent" runat="server">
    <ul>
        <li><%:Html.ActionLink("New Survey", "New", "Surveys", new { area = "Survey" }, null)%></li>
        <li><%:Html.ActionLink("My Surveys", "Index", "Surveys", new { area = "Survey" }, null)%></li>
        <li><%:Html.ActionLink("My Account", "Index", "Account", new { area = string.Empty }, null)%></li>
        <li class="current"><a>Model extensions</a></li>
    </ul>
    <div class="clear">
    </div>
</asp:Content>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">
    <div class="breadcrumbs">
        Extendable Models
    </div><% if (this.Model.ContentModel.Count() == 0) { %>
    <div class="noResults">
        <h3>There are no models that can be updated.</h3>
    </div>
    <% } else { %>
    <%using (Html.BeginForm("Delete", "Surveys")) {%>
    <%:Html.AntiForgeryToken() %>
    <table border="0" cellspacing="0" cellpadding="0" class="tableGrid">
        <tr>
            <th>
                Model
            </th>
            <th>
                Update
            </th>
        </tr>
        <% foreach (var extendableModel in this.Model.ContentModel) %>
        <% { %>
        <tr>
            <td>
                <%=extendableModel%>
            </td>
            <td>
                <%:Html.ActionLink("Update", "UdfIndex", "Account", new { model = extendableModel }, new { })%>
            </td>
        </tr>
        <% } %>
    </table>
    <% } } %>
</asp:Content>

