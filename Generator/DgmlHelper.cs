using System.Xml;

namespace ErikEJ.SqlCeScripting
{
    internal class DgmlHelper
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

        internal void WriteNode(string id, string label, string reference)
        {
            xtw.WriteStartElement("Node");

            xtw.WriteAttributeString("Id", id);
            xtw.WriteAttributeString("Label", label);
            if (!string.IsNullOrEmpty(reference))
                xtw.WriteAttributeString("Reference", reference);

            xtw.WriteEndElement();
        }

        internal void WriteLink(string source, string target, string label)
        {
            xtw.WriteStartElement("Link");

            xtw.WriteAttributeString("Source", source);
            xtw.WriteAttributeString("Target", target);
            if (!string.IsNullOrEmpty(label))
                xtw.WriteAttributeString("Label", label);

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
            xtw.WriteEndElement();
            xtw.Close();
        }
    }
}
