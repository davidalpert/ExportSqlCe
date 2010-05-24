using System.Xml;

namespace ErikEJ.SqlCeScripting
{
    public class DgmlHelper
    {
        private XmlTextWriter xtw;

        public DgmlHelper(string outputFile)
        {
            xtw = new XmlTextWriter(outputFile, System.Text.Encoding.UTF8);
            xtw.Formatting = Formatting.Indented;
            xtw.WriteStartDocument();
            xtw.WriteStartElement("DirectedGraph", "http://schemas.microsoft.com/vs/2009/dgml");
            xtw.WriteAttributeString("GraphDirection", "LeftToRight");
        }

        public void WriteNode(string id, string label)
        {
            xtw.WriteStartElement("Node");

            xtw.WriteAttributeString("Id", id);
            xtw.WriteAttributeString("Label", label);

            xtw.WriteEndElement();
        }

        public void WriteNode(string id, string label, string reference)
        {
            xtw.WriteStartElement("Node");

            xtw.WriteAttributeString("Id", id);
            xtw.WriteAttributeString("Label", label);
            if (!string.IsNullOrEmpty(reference))
                xtw.WriteAttributeString("Reference", reference);

            xtw.WriteEndElement();
        }

        public void WriteLink(string source, string target, string label)
        {
            xtw.WriteStartElement("Link");

            xtw.WriteAttributeString("Source", source);
            xtw.WriteAttributeString("Target", target);
            if (!string.IsNullOrEmpty(label))
                xtw.WriteAttributeString("Label", label);

            xtw.WriteEndElement();
        }

        public void BeginElement(string element)
        {
            xtw.WriteStartElement(element);
        }

        public void EndElement()
        {
            xtw.WriteEndElement();
        }

        public void Close()
        {
            xtw.WriteEndElement();
            xtw.Close();
        }
    }
}
