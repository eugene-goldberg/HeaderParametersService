using System;
using System.Data;
using System.Data.SqlClient;
using System.Xml;

public class HeaderParametersService
{
    private readonly string _connectionString;

    public HeaderParametersService(string connectionString)
    {
        _connectionString = connectionString;
    }

    public DataSet GetHeaderParametersShort(XmlDocument parmIds, DateTime beginDate, DateTime endDate, int tenantId)
    {
        DataSet resultDataSet = new DataSet();

        using (SqlConnection conn = new SqlConnection(_connectionString))
        {
            conn.Open();
            SqlTransaction transaction = conn.BeginTransaction();

            try
            {
                // Prepare temp table to hold header parameters
                DataTable headerPrms = new DataTable();
                headerPrms.Columns.Add("MO_PARAMETER_ID", typeof(int));
                headerPrms.Columns.Add("IS_CALC_TYPE", typeof(int));

                // Insert data into the temp table
                InsertHeaderParameters(conn, transaction, headerPrms, parmIds);

                // Update IS_CALC_TYPE to 2 for parameters using Node expression
                UpdateHeaderParameters(conn, transaction, headerPrms, 2);

                // Update IS_CALC_TYPE to 1 for parameters using Node Type expression
                UpdateHeaderParameters(conn, transaction, headerPrms, 1);

                // Retrieve and add results to dataset
                RetrieveHeaderParameters(conn, transaction, headerPrms, resultDataSet, 0);
                RetrieveHeaderParameters(conn, transaction, headerPrms, resultDataSet, 1);
                RetrieveHeaderParameters(conn, transaction, headerPrms, resultDataSet, 2);

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw new Exception("Error in GetHeaderParametersShort: " + ex.Message);
            }
        }

        return resultDataSet;
    }

    private void InsertHeaderParameters(SqlConnection conn, SqlTransaction transaction, DataTable headerPrms, XmlDocument parmIds)
    {
        string insertQuery = @"
            DECLARE @header_prms TABLE (MO_PARAMETER_ID INT, IS_CALC_TYPE INT);
            INSERT INTO @header_prms (MO_PARAMETER_ID, IS_CALC_TYPE)
            SELECT T.row.value('.', 'int'), 0
            FROM @parmIds.nodes('/p/id') T(row)
            ORDER BY 1 ASC;
            SELECT * FROM @header_prms;";

        using (SqlCommand cmd = new SqlCommand(insertQuery, conn, transaction))
        {
            cmd.Parameters.AddWithValue("@parmIds", parmIds.InnerXml);
            using (SqlDataAdapter da = new SqlDataAdapter(cmd))
            {
                da.Fill(headerPrms);
            }
        }
    }

    private void UpdateHeaderParameters(SqlConnection conn, SqlTransaction transaction, DataTable headerPrms, int calcType)
    {
        string updateQuery = calcType switch
        {
            2 => @"
                UPDATE header_prms
                SET header_prms.IS_CALC_TYPE = 2
                FROM @header_prms header_prms
                INNER JOIN [EA].[PARAMETER_MO] pmo WITH (NOLOCK) ON pmo.[MO_PARAMETER_ID] = header_prms.[MO_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER] p WITH (NOLOCK) ON p.[PARAMETER_ID] = pmo.[REF_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER_EXPRESSION] pe_moc WITH (NOLOCK) ON p.[PARAMETER_ID] = pe_moc.[PARAMETER_ID] AND pe_moc.[MOID] = pmo.[MOID]
                WHERE p.[IS_CALCULATED] = 1;",
            1 => @"
                UPDATE header_prms
                SET header_prms.IS_CALC_TYPE = 1
                FROM @header_prms header_prms
                INNER JOIN [EA].[PARAMETER_MO] pmo WITH (NOLOCK) ON pmo.[MO_PARAMETER_ID] = header_prms.[MO_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER] p WITH (NOLOCK) ON p.[PARAMETER_ID] = pmo.[REF_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER_EXPRESSION] pe_moc WITH (NOLOCK) ON p.[PARAMETER_ID] = pe_moc.[PARAMETER_ID] AND pe_moc.[MOID] IS NULL
                WHERE p.[IS_CALCULATED] = 1
                AND header_prms.IS_CALC_TYPE = 0;",
            _ => throw new ArgumentException("Invalid calcType value")
        };

        using (SqlCommand cmd = new SqlCommand(updateQuery, conn, transaction))
        {
            cmd.ExecuteNonQuery();
        }
    }

    private void RetrieveHeaderParameters(SqlConnection conn, SqlTransaction transaction, DataTable headerPrms, DataSet resultDataSet, int calcType)
    {
        string selectQuery = calcType switch
        {
            0 => @"
                SELECT DISTINCT 
                    0 AS IS_CALC_TYPE,
                    pmo.REF_PARAMETER_ID AS PARAMETER_ID,
                    pmo.MO_PARAMETER_ID AS MO_PARAMETER_ID,
                    RTRIM(p_pv.PICKLIST_VALUE_ID) AS PARMNAME_ID, 
                    RTRIM(p_pv.STORE_VALUE_CHARACTER) AS PARMNAME,
                    ps.PARAMETER_SET_ID AS PARAMETER_SET_ID,
                    RTRIM(ps_pv.PICKLIST_VALUE_ID) AS PS_NAME_ID,
                    RTRIM(ps_pv.STORE_VALUE_CHARACTER) AS PS_NAME,
                    pmo.MOID AS MOID,
                    p.PARAMETER_DATA_TYPE_ID AS PARAMETER_DATA_TYPE_ID,
                    p.PARAMETER_READING_TYPE_ID AS PARAMETER_READING_TYPE_ID,
                    p.PARAMETER_MATERIAL_TYPE_ID AS PARAMETER_MATERIAL_TYPE_ID,
                    ps.PARAMETER_SET_TYPE_ID AS PARAMETER_SET_TYPE_ID,
                    ISNULL(ds.DATA_SOURCE_TYPE_ID, 0) AS DATA_SOURCE_TYPE_ID,
                    ISNULL(ps.DATA_SOURCE_ID, 0) AS DATA_SOURCE_ID,
                    0 AS PARAMETER_EXPRESSION_ID,
                    0 AS EXPRESSION_ID,
                    NULL AS EFF_DATE,
                    0 AS RESULT_MO_PARAMETER_ID,
                    0 AS MAP_VARIABLE_ID,
                    ISNULL(uom_mo.UOM_ID, uom_moc.UOM_ID) AS UOM_ID,
                    ISNULL(uom_mo.UOM_TYPE_ID, uom_moc.UOM_TYPE_ID) AS UOM_TYPE_ID,
                    ps.IS_CONSTANT AS IS_CONSTANT,
                    CASE WHEN ISNULL(pstm.USE_NODEID_MAPPING, -1) = 0 THEN ISNULL(rdp_value.[ColumnName], p.[ENTERED_VALUE_COLUMN_NAME]) ELSE p.ENTERED_VALUE_COLUMN_NAME END AS COLUMN_NAME,
                    CASE WHEN p.PARAMETER_DATA_TYPE_ID = 0 THEN CASE WHEN ISNULL(pstm.USE_NODEID_MAPPING, -1) = 0 THEN ISNULL(rdp_uom.[ColumnName], p.[ENTERED_VALUE_COLUMN_UOM]) ELSE p.[ENTERED_VALUE_COLUMN_UOM] END ELSE '' END AS COLUMN_UOM,
                    NULL AS FORMULA,
                    0 AS NEXT_VARIABLE_ID,
                    1 AS IS_CALC_MAP_COMPLETED,
                    NULL AS EXPRESSION_UOM_ID,
                    NULL AS EXPRESSION_UOM_TYPE_ID,
                    -1 AS EXPRESSION_VARIABLE_ID,
                    NULL AS EXPRESSION_VARIABLE_NAME,
                    0 AS EXPRESSION_VARIABLE_UOM_ID,
                    NULL AS EXPRESSION_NAME,
                    NULL AS EXPRESSION_UOM_NAME,
                    0 AS EVAL_SUBSTITUTED,
                    0 AS EVAL_ESTIMATED,	
                    0 AS EVAL_INVALID,
                    0 AS EVAL_APPROVED,
                    0 AS EVAL_FORECAST
                FROM @header_prms header_prms 
                INNER JOIN [EA].[PARAMETER_MO] pmo WITH (NOLOCK) ON pmo.MO_PARAMETER_ID = header_prms.MO_PARAMETER_ID
                INNER JOIN [EA].[PARAMETER] p WITH (NOLOCK) ON p.[PARAMETER_ID] = pmo.[REF_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER_SET] ps WITH (NOLOCK) ON ps.[PARAMETER_SET_ID] = p.[PARAMETER_SET_ID]
                LEFT JOIN [EA].[DATA_SOURCE] ds WITH (NOLOCK) ON ps.DATA_SOURCE_ID = ds.DATA_SOURCE_ID
                INNER JOIN [EA].[PICKLIST_VALUE] p_pv WITH (NOLOCK) ON p.NAME_PICKLIST_VALUE_ID = p_pv.PICKLIST_VALUE_ID
                INNER JOIN [EA].[PICKLIST_VALUE] ps_pv WITH (NOLOCK) ON ps.NAME_PICKLIST_VALUE_ID = ps_pv.PICKLIST_VALUE_ID
                LEFT JOIN [EA].[PARAMETER_OVERWRITE] po_moc WITH (NOLOCK) ON po_moc.PARAMETER_ID = p.PARAMETER_ID AND po_moc.MOID IS NULL
                LEFT JOIN [EA].[UOM] uom_moc WITH (NOLOCK) ON po_moc.UOM_ID = uom_moc.UOM_ID
                LEFT JOIN [EA].[PARAMETER_OVERWRITE] po_mo WITH (NOLOCK) ON po_mo.PARAMETER_ID = p.PARAMETER_ID AND po_mo.MOID = pmo.MOID
                LEFT JOIN [EA].[UOM] uom_mo WITH (NOLOCK) ON po_mo.UOM_ID = uom_mo.UOM_ID
                LEFT JOIN [EA].[ReferenceDataProperty] rdp_value WITH (NOLOCK) ON rdp_value.[ReferenceDataPropertyId] = p.[ReferenceDataPropertyId] AND rdp_value.[TypeId] = 5
                LEFT JOIN [EA].[ReferenceDataProperty] rdp_uom WITH (NOLOCK) ON rdp_uom.[ParentId] = rdp_value.[ReferenceDataPropertyId] AND rdp_uom.[TypeId] = 6
                WHERE p.[IS_CALCULATED] = 0 AND header_prms.IS_CALC_TYPE = 0;",
            1 => @"
                SELECT DISTINCT 
                    1 AS IS_CALC_TYPE,
                    pmo.REF_PARAMETER_ID AS PARAMETER_ID,
                    pmo.MO_PARAMETER_ID AS MO_PARAMETER_ID,
                    RTRIM(p_pv.PICKLIST_VALUE_ID) AS PARMNAME_ID, 
                    RTRIM(p_pv.STORE_VALUE_CHARACTER) AS PARMNAME,
                    ps.PARAMETER_SET_ID AS PARAMETER_SET_ID,
                    RTRIM(ps_pv.PICKLIST_VALUE_ID) AS PS_NAME_ID,
                    RTRIM(ps_pv.STORE_VALUE_CHARACTER) AS PS_NAME,
                    pmo.MOID AS MOID,
                    p.PARAMETER_DATA_TYPE_ID AS PARAMETER_DATA_TYPE_ID,
                    p.PARAMETER_READING_TYPE_ID AS PARAMETER_READING_TYPE_ID,
                    p.PARAMETER_MATERIAL_TYPE_ID AS PARAMETER_MATERIAL_TYPE_ID,
                    ps.PARAMETER_SET_TYPE_ID AS PARAMETER_SET_TYPE_ID,
                    ISNULL(ds.DATA_SOURCE_TYPE_ID, 0) AS DATA_SOURCE_TYPE_ID,
                    ISNULL(ps.DATA_SOURCE_ID, 0) AS DATA_SOURCE_ID,
                    ISNULL(pe_moc.PARAMETER_EXPRESSION_ID, 0) AS PARAMETER_EXPRESSION_ID,
                    ISNULL(pe_moc.EXPRESSION_ID, 0) AS EXPRESSION_ID,
                    pe_moc.EFFECTIVE_DATE AS EFF_DATE,
                    0 AS RESULT_MO_PARAMETER_ID,
                    0 AS MAP_VARIABLE_ID,
                    ISNULL(uom_mo.UOM_ID, uom_moc.UOM_ID) AS UOM_ID,
                    ISNULL(uom_mo.UOM_TYPE_ID, uom_moc.UOM_TYPE_ID) AS UOM_TYPE_ID,
                    ps.IS_CONSTANT AS IS_CONSTANT,
                    NULL AS COLUMN_NAME,
                    NULL AS COLUMN_UOM,
                    RTRIM(e_moc.FORMULA) AS FORMULA,
                    ISNULL(e_moc.NEXT_VARIABLE_ID, 0) AS NEXT_VARIABLE_ID,
                    1 AS IS_CALC_MAP_COMPLETED,
                    uom_e_moc.UOM_ID AS EXPRESSION_UOM_ID,
                    uom_e_moc.UOM_TYPE_ID AS EXPRESSION_UOM_TYPE_ID,
                    -1 AS EXPRESSION_VARIABLE_ID,
                    NULL AS EXPRESSION_VARIABLE_NAME,
                    0 AS EXPRESSION_VARIABLE_UOM_ID,
                    e_moc.[NAME] AS EXPRESSION_NAME,
                    uom_e_moc.[UOM_NAME] AS EXPRESSION_UOM_NAME,
                    0 AS EVAL_SUBSTITUTED,
                    0 AS EVAL_ESTIMATED,	
                    0 AS EVAL_INVALID,
                    0 AS EVAL_APPROVED,
                    0 AS EVAL_FORECAST
                FROM @header_prms header_prms 
                INNER JOIN [EA].[PARAMETER_MO] pmo WITH (NOLOCK) ON pmo.MO_PARAMETER_ID = header_prms.MO_PARAMETER_ID
                INNER JOIN [EA].[PARAMETER] p WITH (NOLOCK) ON p.[PARAMETER_ID] = pmo.[REF_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER_SET] ps WITH (NOLOCK) ON ps.[PARAMETER_SET_ID] = p.[PARAMETER_SET_ID]
                LEFT JOIN [EA].[DATA_SOURCE] ds WITH (NOLOCK) ON ps.DATA_SOURCE_ID = ds.DATA_SOURCE_ID
                INNER JOIN [EA].[PICKLIST_VALUE] p_pv WITH (NOLOCK) ON p.NAME_PICKLIST_VALUE_ID = p_pv.PICKLIST_VALUE_ID
                INNER JOIN [EA].[PICKLIST_VALUE] ps_pv WITH (NOLOCK) ON ps.NAME_PICKLIST_VALUE_ID = ps_pv.PICKLIST_VALUE_ID
                INNER JOIN [EA].[PARAMETER_EXPRESSION] pe_moc WITH (NOLOCK) ON p.PARAMETER_ID = pe_moc.PARAMETER_ID AND pe_moc.MOID IS NULL
                INNER JOIN [EA].[EXPRESSION] e_moc WITH (NOLOCK) ON pe_moc.EXPRESSION_ID = e_moc.EXPRESSION_ID
                LEFT JOIN [EA].[UOM] uom_e_moc WITH (NOLOCK) ON uom_e_moc.UOM_ID = e_moc.UOM_ID
                LEFT JOIN [EA].[PARAMETER_OVERWRITE] po_moc WITH (NOLOCK) ON po_moc.PARAMETER_ID = p.PARAMETER_ID AND po_moc.MOID IS NULL
                LEFT JOIN [EA].[UOM] uom_moc WITH (NOLOCK) ON po_moc.UOM_ID = uom_moc.UOM_ID
                LEFT JOIN [EA].[PARAMETER_OVERWRITE] po_mo WITH (NOLOCK) ON po_mo.PARAMETER_ID = p.PARAMETER_ID AND po_mo.MOID = pmo.MOID
                LEFT JOIN [EA].[UOM] uom_mo WITH (NOLOCK) ON po_mo.UOM_ID = uom_mo.UOM_ID;",
            2 => @"
                SELECT DISTINCT 
                    2 AS IS_CALC_TYPE,
                    pmo.REF_PARAMETER_ID AS PARAMETER_ID,
                    pmo.MO_PARAMETER_ID AS MO_PARAMETER_ID,
                    RTRIM(p_pv.PICKLIST_VALUE_ID) AS PARMNAME_ID, 
                    RTRIM(p_pv.STORE_VALUE_CHARACTER) AS PARMNAME,
                    ps.PARAMETER_SET_ID AS PARAMETER_SET_ID,
                    RTRIM(ps_pv.PICKLIST_VALUE_ID) AS PS_NAME_ID,
                    RTRIM(ps_pv.STORE_VALUE_CHARACTER) AS PS_NAME,
                    pmo.MOID AS MOID,
                    p.PARAMETER_DATA_TYPE_ID AS PARAMETER_DATA_TYPE_ID,
                    p.PARAMETER_READING_TYPE_ID AS PARAMETER_READING_TYPE_ID,
                    p.PARAMETER_MATERIAL_TYPE_ID AS PARAMETER_MATERIAL_TYPE_ID,
                    ps.PARAMETER_SET_TYPE_ID AS PARAMETER_SET_TYPE_ID,
                    ISNULL(ds.DATA_SOURCE_TYPE_ID, 0) AS DATA_SOURCE_TYPE_ID,
                    ISNULL(ps.DATA_SOURCE_ID, 0) AS DATA_SOURCE_ID,
                    ISNULL(pe_mo.PARAMETER_EXPRESSION_ID, 0) AS PARAMETER_EXPRESSION_ID,
                    ISNULL(pe_mo.EXPRESSION_ID, 0) AS EXPRESSION_ID,
                    pe_mo.EFFECTIVE_DATE AS EFF_DATE,
                    0 AS RESULT_MO_PARAMETER_ID,
                    0 AS MAP_VARIABLE_ID,
                    ISNULL(uom_mo.UOM_ID, uom_moc.UOM_ID) AS UOM_ID,
                    ISNULL(uom_mo.UOM_TYPE_ID, uom_moc.UOM_TYPE_ID) AS UOM_TYPE_ID,
                    ps.IS_CONSTANT AS IS_CONSTANT,
                    NULL AS COLUMN_NAME,
                    NULL AS COLUMN_UOM,
                    RTRIM(e_mo.FORMULA) AS FORMULA,
                    ISNULL(e_mo.NEXT_VARIABLE_ID, 0) AS NEXT_VARIABLE_ID,
                    1 AS IS_CALC_MAP_COMPLETED,
                    uom_e_mo.UOM_ID AS EXPRESSION_UOM_ID,
                    uom_e_mo.UOM_TYPE_ID AS EXPRESSION_UOM_TYPE_ID,
                    -1 AS EXPRESSION_VARIABLE_ID,
                    NULL AS EXPRESSION_VARIABLE_NAME,
                    0 AS EXPRESSION_VARIABLE_UOM_ID,
                    e_mo.[NAME] AS EXPRESSION_NAME,
                    uom_e_mo.[UOM_NAME] AS EXPRESSION_UOM_NAME,
                    0 AS EVAL_SUBSTITUTED,
                    0 AS EVAL_ESTIMATED,	
                    0 AS EVAL_INVALID,
                    0 AS EVAL_APPROVED,
                    0 AS EVAL_FORECAST
                FROM @header_prms header_prms 
                INNER JOIN [EA].[PARAMETER_MO] pmo WITH (NOLOCK) ON pmo.MO_PARAMETER_ID = header_prms.MO_PARAMETER_ID
                INNER JOIN [EA].[PARAMETER] p WITH (NOLOCK) ON p.[PARAMETER_ID] = pmo.[REF_PARAMETER_ID]
                INNER JOIN [EA].[PARAMETER_SET] ps WITH (NOLOCK) ON ps.[PARAMETER_SET_ID] = p.[PARAMETER_SET_ID]
                LEFT JOIN [EA].[DATA_SOURCE] ds WITH (NOLOCK) ON ps.DATA_SOURCE_ID = ds.DATA_SOURCE_ID
                INNER JOIN [EA].[PICKLIST_VALUE] p_pv WITH (NOLOCK) ON p.NAME_PICKLIST_VALUE_ID = p_pv.PICKLIST_VALUE_ID
                INNER JOIN [EA].[PICKLIST_VALUE] ps_pv WITH (NOLOCK) ON ps.NAME_PICKLIST_VALUE_ID = ps_pv.PICKLIST_VALUE_ID
                INNER JOIN [EA].[PARAMETER_EXPRESSION] pe_mo WITH (NOLOCK) ON p.PARAMETER_ID = pe_mo.PARAMETER_ID AND pe_mo.MOID = pmo.MOID
                INNER JOIN [EA].[EXPRESSION] e_mo WITH (NOLOCK) ON pe_mo.EXPRESSION_ID = e_mo.EXPRESSION_ID
                LEFT JOIN [EA].[UOM] uom_e_mo WITH (NOLOCK) ON uom_e_mo.UOM_ID = e_mo.UOM_ID
                LEFT JOIN [EA].[PARAMETER_OVERWRITE] po_moc WITH (NOLOCK) ON po_moc.PARAMETER_ID = p.PARAMETER_ID AND po_moc.MOID IS NULL
                LEFT JOIN [EA].[UOM] uom_moc WITH (NOLOCK) ON po_moc.UOM_ID = uom_moc.UOM_ID
                LEFT JOIN [EA].[PARAMETER_OVERWRITE] po_mo WITH (NOLOCK) ON po_mo.PARAMETER_ID = p.PARAMETER_ID AND po_mo.MOID = pmo.MOID
                LEFT JOIN [EA].[UOM] uom_mo WITH (NOLOCK) ON po_mo.UOM_ID = uom_mo.UOM_ID;",
            _ => throw new ArgumentException("Invalid calcType value")
        };

        using (SqlCommand cmd = new SqlCommand(selectQuery, conn, transaction))
        {
            using (SqlDataAdapter da = new SqlDataAdapter(cmd))
            {
                DataTable resultTable = new DataTable();
                da.Fill(resultTable);
                resultDataSet.Tables.Add(resultTable);
            }
        }
    }
}
