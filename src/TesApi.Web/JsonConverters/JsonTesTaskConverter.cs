using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using TesApi.Models;

namespace TesApi.Web
{
    enum TesView
    {
        MINIMAL,
        BASIC,
        FULL
    }

    class JsonTesTaskConverter : JsonConverter<TesTask>
    {
        private TesView viewLevel;

        public JsonTesTaskConverter(TesView viewLevel)
        {
            this.viewLevel = viewLevel;
        }

        public override TesTask Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        private void WriteEnum<T>(Utf8JsonWriter writer, string propertyName, T value)
        {
            string name = Enum.GetName(typeof(T), value);
            writer.WriteString(propertyName, name.Substring(0, name.IndexOf("Enum")));
        }

        public override void Write(Utf8JsonWriter writer, TesTask value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            writer.WriteString("id", value.Id);
            WriteEnum(writer, "state", value.State);

            if (TesView.MINIMAL != viewLevel)
            {
                writer.WriteString("name", value.Name);
                writer.WriteString("description", value.Description);

                writer.WritePropertyName("inputs");
                writer.WriteStartArray();
                foreach (var input in value.Inputs)
                {
                    writer.WriteStartObject();

                    writer.WriteString("name", input.Name);
                    writer.WriteString("description", input.Description);
                    writer.WriteString("url", input.Url);
                    writer.WriteString("path", input.Path);
                    WriteEnum(writer, "type", input.Type);
                    if (TesView.FULL == viewLevel)
                    {
                        writer.WriteString("content", input.Content);
                    }

                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WritePropertyName("outputs");
                writer.WriteStartArray();
                foreach (var output in value.Outputs)
                {
                    writer.WriteStartObject();

                    writer.WriteString("name", output.Name);
                    writer.WriteString("description", output.Description);
                    writer.WriteString("url", output.Url);
                    writer.WriteString("path", output.Path);
                    WriteEnum(writer, "type", output.Type);

                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WritePropertyName("resources");
                writer.WriteStartObject();
                writer.WriteNumber("cpu_cores", value.Resources.CpuCores.GetValueOrDefault());
                writer.WriteBoolean("preemptible", value.Resources.Preemptible.GetValueOrDefault());
                writer.WriteNumber("ram_gb", value.Resources.RamGb.GetValueOrDefault());
                writer.WriteNumber("disk_gb", value.Resources.DiskGb.GetValueOrDefault());
                writer.WritePropertyName("zones");
                writer.WriteStartArray();
                foreach (var zone in value.Resources.Zones)
                    writer.WriteStringValue(zone);
                writer.WriteEndArray();
                writer.WriteEndObject();

                writer.WritePropertyName("executors");
                writer.WriteStartArray();
                foreach (var executor in value.Executors)
                {
                    writer.WriteStartObject();

                    writer.WriteString("image", executor.Image);
                    writer.WritePropertyName("command");
                    writer.WriteStartArray();
                    foreach (var command in executor.Command)
                        writer.WriteStringValue(command);
                    writer.WriteEndArray();
                    writer.WriteString("workdir", executor.Workdir);
                    writer.WriteString("stdin", executor.Stdin);
                    writer.WriteString("stdout", executor.Stdout);
                    writer.WriteString("stderr", executor.Stderr);
                    writer.WritePropertyName("env");
                    writer.WriteStartObject();
                    foreach (var prop in executor.Env)
                        writer.WriteString(prop.Key, prop.Value);
                    writer.WriteEndObject();

                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WritePropertyName("volumes");
                writer.WriteStartArray();
                foreach (var volume in value.Volumes)
                    writer.WriteStringValue(volume);
                writer.WriteEndArray();

                writer.WritePropertyName("tags");
                writer.WriteStartObject();
                foreach (var tag in value.Tags)
                    writer.WriteString(tag.Key, tag.Value);
                writer.WriteEndObject();

                writer.WritePropertyName("logs");
                writer.WriteStartArray();
                foreach (var log in value.Logs)
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("logs");
                    writer.WriteStartArray();
                    foreach (var executorLog in log.Logs)
                    {
                        writer.WriteStartObject();
                        writer.WriteString("start_time", executorLog.StartTime.GetValueOrDefault());
                        writer.WriteString("end_time", executorLog.EndTime.GetValueOrDefault());
                        if (TesView.FULL == viewLevel)
                        {
                            writer.WriteString("stdout", executorLog.Stdout);
                            writer.WriteString("stderr", executorLog.Stderr);
                        }
                        writer.WriteNumber("exit_code", executorLog.ExitCode.GetValueOrDefault());
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();

                    writer.WritePropertyName("metadata");
                    writer.WriteStartObject();
                    foreach (var prop in log.Metadata)
                        writer.WriteString(prop.Key, prop.Value);
                    writer.WriteEndObject();

                    writer.WriteString("start_time", log.StartTime.GetValueOrDefault());
                    writer.WriteString("end_time", log.EndTime.GetValueOrDefault());

                    writer.WritePropertyName("outputs");
                    writer.WriteStartArray();
                    foreach (var output in log.Outputs)
                    {
                        writer.WriteStartObject();
                        writer.WriteString("url", output.Url);
                        writer.WriteString("path", output.Path);
                        writer.WriteString("size_bytes", output.SizeBytes);
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();

                    if (TesView.FULL == viewLevel)
                    {
                        writer.WritePropertyName("system_logs");
                        writer.WriteStartArray();
                        foreach (var systemLog in log.SystemLogs)
                            writer.WriteStringValue(systemLog);
                        writer.WriteEndArray();
                    }

                    writer.WriteEndObject();
                }
                writer.WriteEndArray();

                writer.WriteString("creation_time", value.CreationTime.GetValueOrDefault());
            }

            writer.WriteEndObject();
        }
    }
}
