using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Npgsql;
using MySql.Data.MySqlClient;
using System.Data;
using CommandLine;
using CommandLine.Text;

// http://www.codeproject.com/Articles/30989/Using-PostgreSQL-in-your-C-NET-application-An-intr

namespace TT_RSS_Migration
{
    class Program
    {
        static void Main(string[] args)
        {
            var options = new Options();
            if (CommandLine.Parser.Default.ParseArguments(args, options) == false)
            {
                return;
            }

			using (var mysqlConnection = new MySqlConnection("default command timeout=0;" + "Server=" + options.MysqlHost + ";Port=" + options.MysqlPort + ";User Id=" + options.MysqlUser + ";Password=" + options.MysqlPassword + ";Database=" + options.MysqlDatabase))
            {
				//mysqlConnection.Server= 0;
                mysqlConnection.Open();

                using (var psqlConnection = new NpgsqlConnection("Server=" + options.PostgresqlHost + ";Port=" + options.PostgresqlPort + ";User Id=" + options.PostgresqlUser + ";Password=" + options.PostgresqlPassword + ";Database=" + options.PostgresqlDatabase))
                {
                    psqlConnection.Open();

                    if (IsSupportedSchema(mysqlConnection) == false)
                    {
                        Console.WriteLine("Schema version not supported");
                        return;
                    }

                    CreateSchema(psqlConnection);

                    CopyTables(
                        new string[] { "ttrss_users", "ttrss_feed_categories", "ttrss_feeds", "ttrss_archived_feeds", "ttrss_counters_cache", "ttrss_cat_counters_cache", "ttrss_entries", "ttrss_user_entries", "ttrss_entry_comments", "ttrss_filter_types", "ttrss_filter_actions", "ttrss_filters2", "ttrss_filters2_rules", "ttrss_filters2_actions", "ttrss_tags", "ttrss_version", "ttrss_enclosures", "ttrss_settings_profiles", "ttrss_prefs_types", "ttrss_prefs", "ttrss_user_prefs", "ttrss_sessions", "ttrss_feedbrowser_cache", "ttrss_labels2", "ttrss_user_labels2", "ttrss_access_keys", "ttrss_linked_instances", "ttrss_linked_feeds", "ttrss_plugin_storage", "ttrss_error_log" },
                        mysqlConnection,
                        psqlConnection);


                    var seriesList = new List<TableSeries>();
                    seriesList.Add(new TableSeries() { TableName = "ttrss_access_keys", ColumnName = "id", SeriesName = "ttrss_access_keys_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_enclosures", ColumnName = "id", SeriesName = "ttrss_enclosures_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_entries", ColumnName = "id", SeriesName = "ttrss_entries_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_entry_comments", ColumnName = "id", SeriesName = "ttrss_entry_comments_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_error_log", ColumnName = "id", SeriesName = "ttrss_error_log_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_feed_categories", ColumnName = "id", SeriesName = "ttrss_feed_categories_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_feeds", ColumnName = "id", SeriesName = "ttrss_feeds_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_filters2_actions", ColumnName = "id", SeriesName = "ttrss_filters2_actions_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_filters2", ColumnName = "id", SeriesName = "ttrss_filters2_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_filters2_rules", ColumnName = "id", SeriesName = "ttrss_filters2_rules_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_labels2", ColumnName = "id", SeriesName = "ttrss_labels2_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_linked_instances", ColumnName = "id", SeriesName = "ttrss_linked_instances_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_plugin_storage", ColumnName = "id", SeriesName = "ttrss_plugin_storage_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_settings_profiles", ColumnName = "id", SeriesName = "ttrss_settings_profiles_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_tags", ColumnName = "id", SeriesName = "ttrss_tags_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_user_entries", ColumnName = "int_id", SeriesName = "ttrss_user_entries_int_id_seq" });
                    seriesList.Add(new TableSeries() { TableName = "ttrss_users", ColumnName = "id", SeriesName = "ttrss_users_id_seq" });
                    UpdateSeries(
                        psqlConnection,
                        "postgres",
                        seriesList);
                }
            }

            Console.WriteLine("migration done");
        }


        private static bool IsSupportedSchema(MySqlConnection mysqlConnection)
        {
            const int SUPPORTED_SCHEMA_VERSION = 124; //ZAP

            using (var sqlCommand = new MySqlCommand("SELECT MAX(schema_version) FROM ttrss_version", mysqlConnection))
            {
                var result = sqlCommand.ExecuteScalar();
                if (result is System.DBNull)
                {
                    return false;
                }
                else
                {
                    var version = (int)result;
                    return (version == SUPPORTED_SCHEMA_VERSION);
                }
            }
        }

        private static void CreateSchema(NpgsqlConnection psqlConnection)
        {
            using (var sqlCommand = new NpgsqlCommand(Resource.PostgreSQL_Schema, psqlConnection))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }

        private static void CopyTables(
            string[] tablesList,
            MySqlConnection mysqlConnection,
            NpgsqlConnection psqlConnection)
        {
            foreach (var table in tablesList)
            {
                using (var mysqlDataAdapter = new MySqlDataAdapter("SELECT * FROM " + table, mysqlConnection))
                {
                using (var mysqlDataSet = new DataSet())
                {
                    mysqlDataSet.Reset();
                    mysqlDataAdapter.Fill(mysqlDataSet);

                    using (var psqlDataAdapter = new NpgsqlDataAdapter("SELECT * FROM " + table, psqlConnection))
                    {
                    using (var psqlDataSet = new DataSet())
                    {
                        psqlDataSet.Reset();
                        psqlDataAdapter.Fill(psqlDataSet);

                        foreach (DataRow srcRow in mysqlDataSet.Tables[0].Rows)
                        {
                            var dstRow = psqlDataSet.Tables[0].NewRow();

                            foreach (DataColumn column in mysqlDataSet.Tables[0].Columns)
                            {
                                if (StringComparer.InvariantCultureIgnoreCase.Compare(table, "ttrss_users") == 0 &&
                                    StringComparer.InvariantCultureIgnoreCase.Compare(column.ColumnName, "theme_id") == 0)
                                    continue;
                                dstRow[column.ColumnName] = srcRow[column.ColumnName];
                            }

                            psqlDataSet.Tables[0].Rows.Add(dstRow);
                        }

                        using (var objCommandBuilder = new NpgsqlCommandBuilder(psqlDataAdapter))
                        {
                            psqlDataAdapter.InsertCommand = objCommandBuilder.GetInsertCommand();
                            psqlDataAdapter.Update(psqlDataSet);
                        }
                    }
                    }
                }
                }
            }
        }

        private static void UpdateSeries(
            NpgsqlConnection psqlConnection,
            string psqlUsername,
            IList<TableSeries> list)
        {
            foreach (var item in list)
            {
                string statement = null;

                using (var sqlCommand = new NpgsqlCommand("SELECT MAX(" + item.ColumnName + ") FROM " + item.TableName, psqlConnection))
                {
                    var result = sqlCommand.ExecuteScalar();
                    if (result is System.DBNull)
                    {
                        statement = string.Format(
                            Resource.PostgreSQL_UpdateSeries_Empty,
                            item.TableName,
                            item.ColumnName,
                            item.SeriesName,
                            psqlUsername);
                    }
                    else
                    {
                        var max = (int)result;

                        statement = string.Format(
                            Resource.PostgreSQL_UpdateSeries,
                            item.TableName,
                            item.ColumnName,
                            item.SeriesName,
                            (max + 1),
                            psqlUsername);
                    }
                }

                using (var sqlCommand = new NpgsqlCommand(statement, psqlConnection))
                {
                    sqlCommand.ExecuteNonQuery();
                }
            }
        }


        private static void GetCommandLineArguments(string[] args)
        {

        }


        struct TableSeries
        {
            public string TableName;
            public string SeriesName;
            public string ColumnName;
        }
    }

    public class Options
    {
        [Option("mhost", Required = true, HelpText = "MySQL host")]
        public string MysqlHost { get; set; }

        [Option("mport", Required = false, HelpText = "MySQL port", DefaultValue = 3306)]
        public int MysqlPort { get; set; }

        [Option("muser", Required = true, HelpText = "MySQL username")]
        public string MysqlUser { get; set; }

        [Option("mpass", Required = true, HelpText = "MySQL password")]
        public string MysqlPassword { get; set; }

        [Option("mdb", Required = true, HelpText = "MySQL database")]
        public string MysqlDatabase { get; set; }

        [Option("phost", Required = true, HelpText = "PostgreSQL host")]
        public string PostgresqlHost { get; set; }

        [Option("pport", Required = false, HelpText = "PostgreSQL port", DefaultValue = 5432)]
        public int PostgresqlPort { get; set; }

        [Option("puser", Required = true, HelpText = "PostgreSQL username")]
        public string PostgresqlUser { get; set; }

        [Option("ppass", Required = true, HelpText = "PostgreSQL password")]
        public string PostgresqlPassword { get; set; }

        [Option("pdb", Required = true, HelpText = "PostgreSQL database")]
        public string PostgresqlDatabase { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(
                this,
                (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}
