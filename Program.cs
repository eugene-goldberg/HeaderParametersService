using System;
using System.Data;
using System.Xml;

class Program
{
    static void Main(string[] args)
    {
        string connectionString = "Your_Connection_String_Here";
        HeaderParametersService service = new HeaderParametersService(connectionString);

        // Sample XML Document
        XmlDocument parmIds = new XmlDocument();
        parmIds.LoadXml("<root><p><id>1</id></p><p><id>2</id></p></root>");

        DateTime beginDate = DateTime.Now.AddDays(-7);
        DateTime endDate = DateTime.Now;
        int tenantId = 1;

        DataSet result = service.GetHeaderParametersShort(parmIds, beginDate, endDate, tenantId);

        // Display the results (for demonstration purposes)
        foreach (DataTable table in result.Tables)
        {
            Console.WriteLine($"Table: {table.TableName}");
            foreach (DataRow row in table.Rows)
            {
                foreach (DataColumn col in table.Columns)
                {
                    Console.Write($"{col.ColumnName}: {row[col]} ");
                }
                Console.WriteLine();
            }
        }
    }
}
