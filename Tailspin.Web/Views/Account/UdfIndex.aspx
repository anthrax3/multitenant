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

<%@ Page Title="" Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Models.TenantUdfPageViewData<IEnumerable<Tailspin.Web.Survey.Shared.DataExtensibility.UDFMetadata>>>" %>

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
        <%: Html.ActionLink("Extendable Models", "ModelIndex", "Account")%> &gt;
        <%: this.Model.Title %>
    </div>
    <% if (this.Model.ContentModel == null || this.Model.ContentModel.Count() == 0) { %>
    <div class="noResults">
        <h3>There are no UDFs defined yet.</h3>
    </div>
    <% } else { %>
    <%using (Html.BeginForm("Delete", "Surveys")) {%>
    <%:Html.AntiForgeryToken() %>
    <table border="0" cellspacing="0" cellpadding="0" class="tableGrid">
        <tr>
            <th>
                Name
            </th>
            <th>
                Display
            </th>
            <th>
                Type
            </th>
            <th>
                Mandatory
            </th>
            <th>
                Default Value
            </th>            
            <th>
                Delete
            </th>
        </tr>
        <% foreach (var udfItem in this.Model.ContentModel) %>
        <% { %>
        <tr>
            <td>
                <%=udfItem.Name %>
            </td>
            <td>
                <%=udfItem.Display %>
            </td>
            <td>
                <%=udfItem.Type %>
            </td>
            <td>
                <%=udfItem.Mandatory %>
            </td>
            <td>
                <%=udfItem.DefaultValue %>
            </td>
            <td>
                <a href="#" onclick="javascript: submitToDelete('<%:Url.Action("UdfDelete", "Account", new { model = this.Model.ModelName, udfName = udfItem.Name })%>')">Delete</a>
            </td>
        </tr>
        <% } %>
    </table>
    <% } } %>
    <table>
        <tr>
            <td>
                <%:Html.ActionLink("Add new field", "UdfNew", "Account", new { model = this.Model.ModelName }, null)%>                
            </td>
        </tr>
    </table>

    <script type="text/javascript">
        function submitToDelete(url) {
            document.forms[0].action = url
            document.forms[0].submit();
        }
    </script>
</asp:Content>

