﻿using System.Xml;
using System;

namespace ErikEJ.SqlCeScripting
{
    internal class DgmlHelper : IDisposable
    {
        private XmlTextWriter xtw;

        internal DgmlHelper(string outputFile)
        {
            xtw = new XmlTextWriter(outputFile, System.Text.Encoding.UTF8);
            xtw.Formatting = Formatting.Indented;
            xtw.WriteStartDocument();
            xtw.WriteStartElement("DirectedGraph", "http://schemas.microsoft.com/vs/2009/dgml");
            xtw.WriteAttributeString("GraphDirection", "LeftToRight");
        }

        internal void WriteNode(string id, string label)
        {
            xtw.WriteStartElement("Node");

            xtw.WriteAttributeString("Id", id);
            xtw.WriteAttributeString("Label", label);

            xtw.WriteEndElement();
        }

        internal void WriteNode(string id, string label, string reference, string category, string group, string description)
        {
            xtw.WriteStartElement("Node");

            xtw.WriteAttributeString("Id", id);
            if (!string.IsNullOrEmpty(label))
                xtw.WriteAttributeString("Label", label);
            if (!string.IsNullOrEmpty(reference))
                xtw.WriteAttributeString("Reference", reference);
            if (!string.IsNullOrEmpty(category))
                xtw.WriteAttributeString("Category", category);
            if (!string.IsNullOrEmpty(group))
                xtw.WriteAttributeString("Group", group);
            if (!string.IsNullOrEmpty(description))
                xtw.WriteAttributeString("Description", description);

            xtw.WriteEndElement();
        }

        internal void WriteLink(string source, string target, string label, string category)
        {
            xtw.WriteStartElement("Link");
            
            xtw.WriteAttributeString("Source", source);
            xtw.WriteAttributeString("Target", target);
            if (!string.IsNullOrEmpty(label))
                xtw.WriteAttributeString("Label", label);
            if (!string.IsNullOrEmpty(category))
                xtw.WriteAttributeString("Category", category);

            xtw.WriteEndElement();
        }

        internal void BeginElement(string element)
        {
            xtw.WriteStartElement(element);
        }

        internal void EndElement()
        {
            xtw.WriteEndElement();
        }

        internal void Close()
        {
            xtw.WriteStartElement("Styles");

            xtw.WriteStartElement("Style");
            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Table");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Table')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FFC0C0C0");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();

            xtw.WriteStartElement("Style");

            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Schema");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Schema')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FF7F9169");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();


            xtw.WriteStartElement("Style");
            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Field Primary");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Field Primary')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FF008000");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();

            xtw.WriteStartElement("Style");
            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Field Optional");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Field Optional')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FF808040");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();

            xtw.WriteStartElement("Style");
            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Field Foreign");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Field Foreign')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FF8080FF");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();

            xtw.WriteStartElement("Style");
            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Field");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Field')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FFC0A000");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();


            xtw.WriteStartElement("Style");
            xtw.WriteAttributeString("TargetType", "Node");
            xtw.WriteAttributeString("GroupLabel", "Database");
            xtw.WriteAttributeString("ValueLabel", "True");

            xtw.WriteStartElement("Condition");
            xtw.WriteAttributeString("Expression", "HasCategory('Database')");
            xtw.WriteEndElement();

            xtw.WriteStartElement("Setter");
            xtw.WriteAttributeString("Property", "Background");
            xtw.WriteAttributeString("Value", "#FFFFFFFF");
            xtw.WriteEndElement();
            //Style end
            xtw.WriteEndElement();


            // Styles end
            xtw.WriteEndElement();

            xtw.WriteEndElement();
            xtw.Close();
        }

        #region IDisposable Members
        public void Dispose()
        {
            Dispose(true);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (xtw != null)
                xtw.Close();
        }
        #endregion
    }
}
