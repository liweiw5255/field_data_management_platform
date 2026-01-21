using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Reflection;
using System.Text;
using System.Linq;
using Gemstone.PQDIF.Logical;
using Gemstone.PQDIF.Physical;

namespace PqdifLogicalExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string inputFile = args[0];
            string outputFile = Path.ChangeExtension(inputFile, ".csv");
            bool debug = false;

            try
            {
                List<ObservationRecord> observationRecords = new List<ObservationRecord>();

                // Now use LogicalParser for actual data reading
                await using LogicalParser parser = new LogicalParser(inputFile);
                await parser.OpenAsync();

                // Deep dive into the logical structure to find all data
                DebugWriteLine(debug, "=== Deep dive into PQDIF logical structure ===\n");
                
                // Check ContainerRecord structure
                try
                {
                    var containerRecord = GetPropertyValue<object>(parser, "ContainerRecord");
                    if (containerRecord != null)
                    {
                        Type containerType = containerRecord.GetType();
                        DebugWriteLine(debug, $"ContainerRecord type: {containerType.Name}");
                        
                        // Explore all properties of ContainerRecord
                        DebugWriteLine(debug, "\nContainerRecord properties:");
                        foreach (var prop in containerType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                        {
                            try
                            {
                                var propVal = prop.GetValue(containerRecord);
                                if (propVal != null)
                                {
                                    if (propVal is System.Collections.IEnumerable propEnum && !(propVal is string))
                                    {
                                        int count = 0;
                                        foreach (var _ in propEnum) { count++; if (count > 10) break; }
                                        DebugWriteLine(debug, $"  {prop.Name}: {propVal.GetType().Name} (count: {count})");
                                    }
                                    else
                                    {
                                        DebugWriteLine(debug, $"  {prop.Name}: {propVal} ({propVal.GetType().Name})");
                                    }
                                }
                            }
                            catch { }
                        }
                        
                        // Try to get DataSourceRecords
                        var dataSourceRecords = GetPropertyValue<object>(containerRecord, "DataSourceRecords");
                        if (dataSourceRecords != null && dataSourceRecords is System.Collections.IEnumerable dsEnum)
                        {
                            int dsCount = 0;
                            foreach (var dsr in dsEnum)
                            {
                                dsCount++;
                                DebugWriteLine(debug, $"\n=== DataSourceRecord {dsCount} ===");
                                Type dsrType = dsr.GetType();
                                DebugWriteLine(debug, $"Type: {dsrType.Name}");
                                
                                // Explore DataSourceRecord properties
                                foreach (var prop in dsrType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                {
                                    try
                                    {
                                        var propVal = prop.GetValue(dsr);
                                        if (propVal != null)
                                        {
                                            if (propVal is System.Collections.IEnumerable propEnum && !(propVal is string))
                                            {
                                                int count = 0;
                                                foreach (var _ in propEnum) { count++; if (count > 10) break; }
                                                DebugWriteLine(debug, $"  {prop.Name}: {propVal.GetType().Name} (count: {count})");
                                                
                                                // If it's observation records, add them
                                                if (prop.Name.Contains("Observation"))
                                                {
                                                    foreach (var obs in propEnum)
                                                    {
                                                        if (obs is ObservationRecord obsRec && !observationRecords.Contains(obsRec))
                                                        {
                                                            observationRecords.Add(obsRec);
                                                        }
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                DebugWriteLine(debug, $"  {prop.Name}: {propVal} ({propVal.GetType().Name})");
                                            }
                                        }
                                    }
                                    catch { }
                                }
                                
                                if (dsCount >= 3) break; // Limit output
                            }
                            DebugWriteLine(debug, $"\nTotal DataSourceRecords found: {dsCount}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    DebugWriteLine(debug, $"Error exploring ContainerRecord: {ex.Message}\n{ex.StackTrace}");
                }
               
                // Also read observation records via HasNextObservationRecordAsync (current method)
                DebugWriteLine(debug, "\n=== Reading observation records via HasNextObservationRecordAsync ===");
                int obsCountRead = 0;
                
                // Create a new parser instance to read observation records sequentially
                await using LogicalParser parser2 = new LogicalParser(inputFile);
                await parser2.OpenAsync();
                
                while (await parser2.HasNextObservationRecordAsync())
                {
                    ObservationRecord record = await parser2.NextObservationRecordAsync();
                    
                    // Only add if not already in list (use reference comparison since Contains might not work)
                    bool alreadyExists = false;
                    var newObsTime = GetPropertyValue<DateTime?>(record, "TimeCreated") ?? 
                                    GetPropertyValue<DateTime?>(record, "StartTime");
                    
                    foreach (var existingRecord in observationRecords)
                    {
                        var existingObsTime = GetPropertyValue<DateTime?>(existingRecord, "TimeCreated") ?? 
                                             GetPropertyValue<DateTime?>(existingRecord, "StartTime");
                        if (existingObsTime == newObsTime && existingRecord.ChannelInstances.Count == record.ChannelInstances.Count)
                        {
                            alreadyExists = true;
                            break;
                        }
                    }
                    
                    if (!alreadyExists)
                    {
                   observationRecords.Add(record);
                        obsCountRead++;
                        
                        // Print info about each observation record as we read it
                        DebugWriteLine(debug, $"  Read observation {obsCountRead}: Time={newObsTime}, Channels={record.ChannelInstances.Count}");
                    }
                }
                
                DebugWriteLine(debug, $"\nTotal observation records: {observationRecords.Count}");
 
                // Extract data using reflection to safely access properties
                List<Dictionary<string, object>> allDataRows = new List<Dictionary<string, object>>();
                HashSet<string> allColumns = new HashSet<string> { "Time" };
                HashSet<string> columnsWithMeaningfulValues = new HashSet<string>();

                DebugWriteLine(debug, $"\n=== Processing {observationRecords.Count} observation records ===\n");
                
                // First pass: find the maximum number of values across all series using GetValues()
                int maxValuesPerSeries = 0;
                foreach (var observation in observationRecords)
                {
                    foreach (var channelInstance in observation.ChannelInstances)
                    {
                        foreach (var seriesInstance in channelInstance.SeriesInstances)
                        {
                            // Try to get values using GetValues() method from VectorElement
                            object? seriesValuesObj = GetPropertyValue<object>(seriesInstance, "SeriesValues");
                            if (seriesValuesObj != null)
                            {
                                Type vectorType = seriesValuesObj.GetType();
                                var getValuesMethod = vectorType.GetMethod("GetValues", new Type[0]);
                                if (getValuesMethod != null)
                                {
                                    try
                                    {
                                        var result = getValuesMethod.Invoke(seriesValuesObj, null);
                                        if (result != null && result is System.Collections.IEnumerable methodEnum)
                                        {
                                            int count = methodEnum.Cast<object?>().Count();
                                            if (count > maxValuesPerSeries)
                                                maxValuesPerSeries = count;
                                        }
                                    }
                                    catch { }
                                }
                            }
                            
                            // Fallback to OriginalValues if GetValues() didn't work
                            if (maxValuesPerSeries == 0)
                            {
                                object? valuesObj = GetPropertyValue<object>(seriesInstance, "OriginalValues");
                                if (valuesObj != null && valuesObj is System.Collections.IEnumerable enumerable)
                                {
                                    int count = enumerable.Cast<object?>().Count();
                                    if (count > maxValuesPerSeries)
                                        maxValuesPerSeries = count;
                                }
                            }
                        }
                    }
                }
                
                DebugWriteLine(debug, $"Maximum values per series: {maxValuesPerSeries}");
                DebugWriteLine(debug, $"This suggests {maxValuesPerSeries} rows should be created\n");
                
                int observationIndex = 0;
                foreach (var observation in observationRecords)
                {
                    // Get observation timestamp
                    DateTime? obsTime = GetPropertyValue<DateTime?>(observation, "TimeCreated") ?? 
                                       GetPropertyValue<DateTime?>(observation, "StartTime");
                    
                    DebugWriteLine(debug, $"\n--- Observation Record {observationIndex} ---");
                    DebugWriteLine(debug, $"  Timestamp: {obsTime}");
                    DebugWriteLine(debug, $"  ChannelInstances count: {observation.ChannelInstances.Count}");
                    
                    int channelIndex = 0;
                    foreach (var channelInstance in observation.ChannelInstances)
                    {
                        // Get channel name and phase information
                        string channelName = "";
                        string channelPhase = ""; // AN, BN, CN, etc.
                        string channelQuantity = ""; // Current, Voltage, Power, etc.
                        // Try both "ChannelDefinition" and "Definition" - the property is actually called "Definition"
                        var channelDef = GetPropertyValue<object>(channelInstance, "Definition") ??
                                        GetPropertyValue<object>(channelInstance, "ChannelDefinition");
                        
                        // Debug: Check if channelDef is null
                        if (observationIndex == 0 && channelIndex < 3)
                        {
                            DebugWriteLine(debug, $"\n  Channel {channelIndex}: channelDef is {(channelDef == null ? "NULL" : "NOT NULL")} (Type: {channelDef?.GetType().Name ?? "null"})");
                            
                            // List all properties on ChannelInstance to see what's available
                            DebugWriteLine(debug, $"\n  === ChannelInstance properties for Channel {channelIndex} ===");
                            Type instanceType = channelInstance.GetType();
                            foreach (var prop in instanceType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                            {
                                try
                                {
                                    var propVal = prop.GetValue(channelInstance);
                                    if (propVal != null)
                                    {
                                        string valStr = propVal.ToString() ?? "";
                                        if (valStr.Length > 100) valStr = valStr.Substring(0, 100) + "...";
                                        DebugWriteLine(debug, $"    {prop.Name}: {valStr} ({propVal.GetType().Name})");
                                    }
                                }
                                catch { }
                            }
                        }
                        
                        // Also try to get channel name directly from ChannelInstance
                        var channelInstanceName = GetPropertyValue<string>(channelInstance, "Name") ??
                                                 GetPropertyValue<string>(channelInstance, "Description") ??
                                                 GetPropertyValue<string>(channelInstance, "Label");
                        
                        if (!string.IsNullOrEmpty(channelInstanceName))
                        {
                            channelName = channelInstanceName.Trim();
                            if (observationIndex == 0 && channelIndex < 3)
                            {
                                DebugWriteLine(debug, $"    Using ChannelInstance.Name: '{channelName}'");
                            }
                        }
                        
                        // Try to get quantity and phase from ChannelInstance directly
                        var qtyFromInstance = GetPropertyValue<object>(channelInstance, "QuantityMeasured") ??
                                            GetPropertyValue<object>(channelInstance, "Quantity") ??
                                            GetPropertyValue<object>(channelInstance, "MeasuredQuantity");
                        if (qtyFromInstance != null && string.IsNullOrEmpty(channelQuantity))
                        {
                            string qtyStr = qtyFromInstance.ToString() ?? "";
                            if (qtyFromInstance.GetType().IsEnum)
                            {
                                qtyStr = qtyFromInstance.GetType().GetEnumName(qtyFromInstance) ?? qtyStr;
                            }
                            if (qtyStr.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                channelQuantity = "Current";
                            else if (qtyStr.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                channelQuantity = "Voltage";
                            else if (qtyStr.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                channelQuantity = "Power";
                            else
                                channelQuantity = qtyStr;
                                
                            if (observationIndex == 0 && channelIndex < 3)
                            {
                                DebugWriteLine(debug, $"    Found quantity from ChannelInstance: '{qtyStr}' -> '{channelQuantity}'");
                            }
                        }
                        
                        var phaseFromInstance = GetPropertyValue<object>(channelInstance, "Phase") ??
                                              GetPropertyValue<object>(channelInstance, "PhaseType") ??
                                              GetPropertyValue<object>(channelInstance, "Connection");
                        if (phaseFromInstance != null && string.IsNullOrEmpty(channelPhase))
                        {
                            string phaseStr = phaseFromInstance.ToString() ?? "";
                            if (phaseFromInstance.GetType().IsEnum)
                            {
                                phaseStr = phaseFromInstance.GetType().GetEnumName(phaseFromInstance) ?? phaseStr;
                            }
                            if (phaseStr.Contains("AN", StringComparison.OrdinalIgnoreCase))
                                channelPhase = "AN";
                            else if (phaseStr.Contains("BN", StringComparison.OrdinalIgnoreCase))
                                channelPhase = "BN";
                            else if (phaseStr.Contains("CN", StringComparison.OrdinalIgnoreCase))
                                channelPhase = "CN";
                            else if (phaseStr.Contains("NG", StringComparison.OrdinalIgnoreCase))
                                channelPhase = "NG";
                                
                            if (observationIndex == 0 && channelIndex < 3 && !string.IsNullOrEmpty(channelPhase))
                            {
                                DebugWriteLine(debug, $"    Found Phase from ChannelInstance: '{phaseStr}' -> '{channelPhase}'");
                            }
                        }
                        
                        if (channelDef != null)
                        {
                            // Debug: List all properties on ChannelDefinition for first few channels
                            if (observationIndex == 0 && channelIndex < 3)
                            {
                                DebugWriteLine(debug, $"\n  === ChannelDefinition properties for Channel {channelIndex} ===");
                                Type defType = channelDef.GetType();
                                foreach (var prop in defType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                {
                                    try
                                    {
                                        var propVal = prop.GetValue(channelDef);
                                        if (propVal != null)
                                        {
                                            DebugWriteLine(debug, $"    {prop.Name}: {propVal} ({propVal.GetType().Name})");
                                        }
                                    }
                                    catch { }
                                }
                            }
                            
                            // Try multiple properties to get the channel name
                            var name = GetPropertyValue<string>(channelDef, "Name") ??
                                      GetPropertyValue<string>(channelDef, "Description") ??
                                      GetPropertyValue<string>(channelDef, "Label") ??
                                      GetPropertyValue<string>(channelDef, "ChannelName") ??
                                      GetPropertyValue<string>(channelDef, "Title");
                            
                            if (!string.IsNullOrEmpty(name))
                            {
                                channelName = name.Trim();
                                if (observationIndex == 0 && channelIndex < 3)
                                {
                                    DebugWriteLine(debug, $"    Using channel name: '{channelName}'");
                                }
                                
                                // Check if name contains phase information (AN, BN, CN, A, B, C, N, etc.)
                                if (name.Contains("AN", StringComparison.OrdinalIgnoreCase) || name.Contains("A-N", StringComparison.OrdinalIgnoreCase))
                                    channelPhase = "AN";
                                else if (name.Contains("BN", StringComparison.OrdinalIgnoreCase) || name.Contains("B-N", StringComparison.OrdinalIgnoreCase))
                                    channelPhase = "BN";
                                else if (name.Contains("CN", StringComparison.OrdinalIgnoreCase) || name.Contains("C-N", StringComparison.OrdinalIgnoreCase))
                                    channelPhase = "CN";
                                else if (name.Contains("NG", StringComparison.OrdinalIgnoreCase) || name.Contains("N-G", StringComparison.OrdinalIgnoreCase))
                                    channelPhase = "NG";
                                else if (name.Contains("LineToNeutralAverage", StringComparison.OrdinalIgnoreCase))
                                    channelPhase = "LineToNeutralAverage";
                                else if (name.Contains("A") && !name.Contains("B") && !name.Contains("C") && !name.Contains("AN") && !name.Contains("NG"))
                                    channelPhase = "A";
                                else if (name.Contains("B") && !name.Contains("A") && !name.Contains("C") && !name.Contains("BN"))
                                    channelPhase = "B";
                                else if (name.Contains("C") && !name.Contains("A") && !name.Contains("B") && !name.Contains("CN"))
                                    channelPhase = "C";
                            }
                            
                            // Also check Phase property if it exists - try multiple property names
                            if (string.IsNullOrEmpty(channelPhase))
                            {
                                var phase = GetPropertyValue<object>(channelDef, "Phase") ??
                                          GetPropertyValue<object>(channelDef, "PhaseType") ??
                                          GetPropertyValue<object>(channelDef, "Connection") ??
                                          GetPropertyValue<object>(channelDef, "ConnectionType");
                                if (phase != null)
                                {
                                    string phaseStr = phase.ToString() ?? "";
                                    if (phase.GetType().IsEnum)
                                    {
                                        phaseStr = phase.GetType().GetEnumName(phase) ?? phaseStr;
                                    }
                                    if (phaseStr.Contains("AN", StringComparison.OrdinalIgnoreCase) || phaseStr.Contains("A-N", StringComparison.OrdinalIgnoreCase))
                                        channelPhase = "AN";
                                    else if (phaseStr.Contains("BN", StringComparison.OrdinalIgnoreCase) || phaseStr.Contains("B-N", StringComparison.OrdinalIgnoreCase))
                                        channelPhase = "BN";
                                    else if (phaseStr.Contains("CN", StringComparison.OrdinalIgnoreCase) || phaseStr.Contains("C-N", StringComparison.OrdinalIgnoreCase))
                                        channelPhase = "CN";
                                    else if (phaseStr.Contains("NG", StringComparison.OrdinalIgnoreCase) || phaseStr.Contains("N-G", StringComparison.OrdinalIgnoreCase))
                                        channelPhase = "NG";
                                    else if (phaseStr.Contains("LineToNeutralAverage", StringComparison.OrdinalIgnoreCase))
                                        channelPhase = "LineToNeutralAverage";
                                        
                                    if (observationIndex == 0 && channelIndex < 3 && !string.IsNullOrEmpty(channelPhase))
                                    {
                                        DebugWriteLine(debug, $"    Found Phase from ChannelDefinition: '{phaseStr}' -> '{channelPhase}'");
                                    }
                                }
                            }
                        }
                        
                        // If still no phase, try to infer from channel index pattern
                        // Common pattern: channels 0-2 = A/B/C or AN/BN/CN, then repeat for next quantity
                        if (string.IsNullOrEmpty(channelPhase))
                        {
                            int phaseIndex = channelIndex % 3;
                            if (phaseIndex == 0) channelPhase = "AN";
                            else if (phaseIndex == 1) channelPhase = "BN";
                            else if (phaseIndex == 2) channelPhase = "CN";
                        }
                        
                        // If we still don't have a channel name, try to construct it from QuantityMeasured and Phase
                        if (string.IsNullOrEmpty(channelName) && channelDef != null)
                        {
                            var qtyMeasured = GetPropertyValue<object>(channelDef, "QuantityMeasured");
                            string qtyStr = "";
                            
                            // Debug: Print QuantityMeasured for first few channels
                            if (observationIndex == 0 && channelIndex < 3)
                            {
                                DebugWriteLine(debug, $"    ChannelDefinition.QuantityMeasured: {qtyMeasured} (Type: {qtyMeasured?.GetType().Name ?? "null"})");
                            }
                            
                            if (qtyMeasured != null)
                            {
                                qtyStr = qtyMeasured.ToString() ?? "";
                                if (qtyMeasured.GetType().IsEnum)
                                {
                                    qtyStr = qtyMeasured.GetType().GetEnumName(qtyMeasured) ?? qtyStr;
                                }
                                
                                if (observationIndex == 0 && channelIndex < 3)
                                {
                                    DebugWriteLine(debug, $"    Extracted qtyStr: '{qtyStr}'");
                                }
                                
                                // Map quantity types to readable names
                                if (qtyStr.Equals("Current", StringComparison.OrdinalIgnoreCase) ||
                                    qtyStr.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                    qtyStr = "Current";
                                else if (qtyStr.Equals("Voltage", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                    qtyStr = "Voltage";
                                else if (qtyStr.Equals("Power", StringComparison.OrdinalIgnoreCase) || 
                                         qtyStr.Contains("Power", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Apparent", StringComparison.OrdinalIgnoreCase))
                                    qtyStr = "Power";
                                else if (qtyStr.Contains("Active", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Real", StringComparison.OrdinalIgnoreCase))
                                    qtyStr = "Power";
                                else if (qtyStr.Contains("Reactive", StringComparison.OrdinalIgnoreCase))
                                    qtyStr = "Power";
                            }
                            
                            // Also check if ChannelDefinition.Name exists but we missed it
                            if (string.IsNullOrEmpty(qtyStr))
                            {
                                var chDefName = GetPropertyValue<string>(channelDef, "Name");
                                if (!string.IsNullOrEmpty(chDefName))
                                {
                                    if (observationIndex == 0 && channelIndex < 3)
                                    {
                                        DebugWriteLine(debug, $"    ChannelDefinition.Name: '{chDefName}'");
                                    }
                                    
                                    // Check if it contains quantity info
                                    if (chDefName.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Current";
                                    else if (chDefName.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Voltage";
                                    else if (chDefName.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Power";
                                }
                            }
                            
                            // Also check other properties for quantity info
                            if (string.IsNullOrEmpty(qtyStr))
                            {
                                var chDefDesc = GetPropertyValue<string>(channelDef, "Description");
                                var chDefLabel = GetPropertyValue<string>(channelDef, "Label");
                                
                                if (!string.IsNullOrEmpty(chDefDesc))
                                {
                                    if (chDefDesc.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Current";
                                    else if (chDefDesc.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Voltage";
                                    else if (chDefDesc.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Power";
                                }
                                
                                if (string.IsNullOrEmpty(qtyStr) && !string.IsNullOrEmpty(chDefLabel))
                                {
                                    if (chDefLabel.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Current";
                                    else if (chDefLabel.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Voltage";
                                    else if (chDefLabel.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                        qtyStr = "Power";
                                }
                            }
                            
                            // Construct name from quantity and phase
                            if (!string.IsNullOrEmpty(qtyStr))
                            {
                                if (!string.IsNullOrEmpty(channelPhase))
                                {
                                    channelName = $"{qtyStr} {channelPhase}";
                                }
                                else
                                {
                                    channelName = qtyStr;
                                }
                                
                                if (observationIndex == 0 && channelIndex < 3)
                                {
                                    DebugWriteLine(debug, $"    Constructed channel name from QuantityMeasured: '{channelName}'");
                                }
                            }
                        }
                        
                        // Get quantity from ChannelDefinition (Current, Voltage, Power, etc.)
                        // This is needed to construct channel name
                        // (channelQuantity already declared above)
                        
                        // First try ChannelDefinition
                        if (channelDef != null)
                        {
                            // Try multiple property names for QuantityMeasured
                            var qtyMeasured = GetPropertyValue<object>(channelDef, "QuantityMeasured") ??
                                            GetPropertyValue<object>(channelDef, "Quantity") ??
                                            GetPropertyValue<object>(channelDef, "MeasuredQuantity") ??
                                            GetPropertyValue<object>(channelDef, "QuantityType");
                            
                            if (qtyMeasured != null)
                            {
                                string qtyStr = qtyMeasured.ToString() ?? "";
                                if (qtyMeasured.GetType().IsEnum)
                                {
                                    qtyStr = qtyMeasured.GetType().GetEnumName(qtyMeasured) ?? qtyStr;
                                }
                                
                                // Map to readable quantity names (Current, Voltage, Power)
                                if (qtyStr.Equals("Current", StringComparison.OrdinalIgnoreCase) || 
                                    qtyStr.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                    channelQuantity = "Current";
                                else if (qtyStr.Equals("Voltage", StringComparison.OrdinalIgnoreCase) || 
                                         qtyStr.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                    channelQuantity = "Voltage";
                                else if (qtyStr.Equals("Power", StringComparison.OrdinalIgnoreCase) || 
                                         qtyStr.Contains("Power", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Apparent", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Active", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Real", StringComparison.OrdinalIgnoreCase) ||
                                         qtyStr.Contains("Reactive", StringComparison.OrdinalIgnoreCase))
                                    channelQuantity = "Power";
                                else
                                    channelQuantity = qtyStr; // Use as-is if not recognized
                                    
                                if (observationIndex == 0 && channelIndex < 3)
                                {
                                    DebugWriteLine(debug, $"    Found QuantityMeasured: '{qtyStr}' -> '{channelQuantity}'");
                                }
                            }
                            
                            // If channelQuantity still empty, try to get from ChannelDefinition name
                            if (string.IsNullOrEmpty(channelQuantity))
                            {
                                var chName = GetPropertyValue<string>(channelDef, "Name") ??
                                           GetPropertyValue<string>(channelDef, "Description") ??
                                           GetPropertyValue<string>(channelDef, "Label");
                                if (!string.IsNullOrEmpty(chName))
                                {
                                    // Extract quantity from name
                                    if (chName.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                        channelQuantity = "Current";
                                    else if (chName.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                        channelQuantity = "Voltage";
                                    else if (chName.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                        channelQuantity = "Power";
                                    else
                                        channelQuantity = chName;
                                        
                                    if (observationIndex == 0 && channelIndex < 3)
                                    {
                                        DebugWriteLine(debug, $"    Extracted quantity from ChannelDefinition name: '{chName}' -> '{channelQuantity}'");
                                    }
                                }
                            }
                        }
                        
                        // Construct channel name from quantity and phase if we don't have one yet
                        if (string.IsNullOrEmpty(channelName))
                        {
                            if (!string.IsNullOrEmpty(channelQuantity))
                            {
                                // Determine phase/connection suffix
                                string phaseSuffix = "";
                                if (!string.IsNullOrEmpty(channelPhase))
                                {
                                    if (channelPhase == "LineToNeutralAverage")
                                        phaseSuffix = "LineToNeutralAverage";
                                    else if (channelPhase == "NG")
                                        phaseSuffix = "NG";
                                    else if (channelPhase == "AN" || channelPhase == "BN" || channelPhase == "CN")
                                        phaseSuffix = channelPhase;
                                    else if (channelQuantity == "Power")
                                        phaseSuffix = "Total"; // Power channels often have "Total"
                                    else
                                        phaseSuffix = channelPhase;
                                    
                                    channelName = $"{channelQuantity} {phaseSuffix}";
                                }
                                else
                                {
                                    channelName = channelQuantity;
                                }
                                
                                if (observationIndex == 0 && channelIndex < 3)
                                {
                                    DebugWriteLine(debug, $"    Constructed channel name from QuantityMeasured: '{channelName}'");
                                }
                            }
                        }
                        
                        // If we still don't have a channel name, use a fallback
                        if (string.IsNullOrEmpty(channelName))
                        {
                            channelName = $"Channel_{channelIndex}";
                        }
                        
                        // Debug: Print channel name if it's one of the first few channels
                        if (observationIndex == 0 && channelIndex < 3)
                        {
                            DebugWriteLine(debug, $"  Channel {channelIndex}: Final Name='{channelName}', Phase='{channelPhase}', Quantity='{channelQuantity}'");
                        }

                        int seriesIndex = 0;
                        foreach (var seriesInstance in channelInstance.SeriesInstances)
                        {
                            // Get values - declare variables first
                            object? seriesValuesObj = GetPropertyValue<object>(seriesInstance, "SeriesValues");
                            
                            IEnumerable<object?>? values = null;
                            int valueCount = 0;
                            List<object?>? collectionValuesFromPhysical = null;
                            
                            // According to the PQDIF structure reference, we need to access collections and elements
                            // SeriesInstance.PhysicalStructure is a Collection - we need to access its elements
                            object? physicalStructure = GetPropertyValue<object>(seriesInstance, "PhysicalStructure");
                            if (physicalStructure != null)
                            {
                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                {
                                    DebugWriteLine(debug, $"\n=== Channel {channelIndex}, Series {seriesIndex} - Accessing PhysicalStructure ===");
                                }
                                
                                // PhysicalStructure is a CollectionElement - check if it has Elements or Items
                                Type physType = physicalStructure.GetType();
                                
                                // Try to get elements from the collection
                                var elementsProp = physType.GetProperty("Elements");
                                var itemsProp = physType.GetProperty("Items");
                                var getElementsMethod = physType.GetMethod("GetElements");
                                var getItemsMethod = physType.GetMethod("GetItems");
                                
                                // Try accessing elements/items from PhysicalStructure collection
                                System.Collections.IEnumerable? collectionElements = null;
                                
                                if (elementsProp != null)
                                {
                                    var elements = elementsProp.GetValue(physicalStructure);
                                    if (elements is System.Collections.IEnumerable elems)
                                        collectionElements = elems;
                                }
                                else if (itemsProp != null)
                                {
                                    var items = itemsProp.GetValue(physicalStructure);
                                    if (items is System.Collections.IEnumerable itms)
                                        collectionElements = itms;
                                }
                                else if (getElementsMethod != null)
                                {
                                    var elements = getElementsMethod.Invoke(physicalStructure, null);
                                    if (elements is System.Collections.IEnumerable elems)
                                        collectionElements = elems;
                                }
                                else if (getItemsMethod != null)
                                {
                                    var items = getItemsMethod.Invoke(physicalStructure, null);
                                    if (items is System.Collections.IEnumerable itms)
                                        collectionElements = itms;
                                }
                                
                                // If we have collection elements, process them
                                if (collectionElements != null)
                                {
                                    collectionValuesFromPhysical = new List<object?>();
                                    int elementIndex = 0;
                                    
                                    foreach (var element in collectionElements)
                                    {
                                        elementIndex++;
                                        
                                        // Try to get value from element
                                        if (element != null)
                                        {
                                            Type elementType = element.GetType();
                                            
                                            // Check for Value property
                                            var valueProp = elementType.GetProperty("Value");
                                            if (valueProp != null)
                                            {
                                                var elementValue = valueProp.GetValue(element);
                                                collectionValuesFromPhysical.Add(elementValue);
                                            }
                                            // Check for Values property (might be a collection)
                                            else
                                            {
                                                var valuesProp = elementType.GetProperty("Values");
                                                if (valuesProp != null)
                                                {
                                                    var elementValues = valuesProp.GetValue(element);
                                                    if (elementValues is System.Collections.IEnumerable valEnum)
                                                    {
                                                        foreach (var val in valEnum)
                                                        {
                                                            collectionValuesFromPhysical.Add(val);
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    collectionValuesFromPhysical.Add(element);
                                                }
                                            }
                                            
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && elementIndex <= 5)
                                            {
                                                DebugWriteLine(debug, $"  Collection element {elementIndex - 1}: {element} (type: {elementType.Name})");
                                            }
                                        }
                                    }
                                    
                                    if (collectionValuesFromPhysical.Count > 50)
                                    {
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"  *** Found {collectionValuesFromPhysical.Count} values from PhysicalStructure collection! ***");
                                        }
                                        values = collectionValuesFromPhysical;
                                        valueCount = collectionValuesFromPhysical.Count;
                                    }
                                }
                            }
                            
                            // Build meaningful column name from PQDIF metadata
                            string columnName = "";
                            var seriesDef = GetPropertyValue<object>(seriesInstance, "Definition") ?? 
                                           GetPropertyValue<object>(seriesInstance, "SeriesDefinition");
                            
                            // Extract quantity and phase from SeriesDefinition if we don't have them yet
                            if (seriesDef != null && (string.IsNullOrEmpty(channelQuantity) || string.IsNullOrEmpty(channelPhase)))
                            {
                                if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0)
                                {
                                    DebugWriteLine(debug, $"\n  === SeriesDefinition properties for Channel {channelIndex}, Series {seriesIndex} ===");
                                    Type seriesDefType = seriesDef.GetType();
                                    foreach (var prop in seriesDefType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                    {
                                        try
                                        {
                                            var propVal = prop.GetValue(seriesDef);
                                            if (propVal != null)
                                            {
                                                string valStr = propVal.ToString() ?? "";
                                                if (valStr.Length > 100) valStr = valStr.Substring(0, 100) + "...";
                                                DebugWriteLine(debug, $"    {prop.Name}: {valStr} ({propVal.GetType().Name})");
                                            }
                                        }
                                        catch { }
                                    }
                                }
                                
                                // Get ChannelDefinition from SeriesDefinition - this is where the channel info is!
                                var channelDefFromSeries = GetPropertyValue<object>(seriesDef, "ChannelDefinition");
                                if (channelDefFromSeries != null)
                                {
                                    // If we don't have channelDef yet, use this one
                                    if (channelDef == null)
                                    {
                                        channelDef = channelDefFromSeries;
                                        if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0)
                                        {
                                            DebugWriteLine(debug, $"    Found ChannelDefinition from SeriesDefinition!");
                                        }
                                    }
                                    
                                    // Extract quantity from ChannelDefinition
                                    if (string.IsNullOrEmpty(channelQuantity))
                                    {
                                        var qtyFromChannelDef = GetPropertyValue<object>(channelDefFromSeries, "QuantityMeasured") ??
                                                               GetPropertyValue<object>(channelDefFromSeries, "Quantity") ??
                                                               GetPropertyValue<object>(channelDefFromSeries, "MeasuredQuantity");
                                        if (qtyFromChannelDef != null)
                                        {
                                            string qtyStr = qtyFromChannelDef.ToString() ?? "";
                                            if (qtyFromChannelDef.GetType().IsEnum)
                                            {
                                                qtyStr = qtyFromChannelDef.GetType().GetEnumName(qtyFromChannelDef) ?? qtyStr;
                                            }
                                            if (qtyStr.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                                channelQuantity = "Current";
                                            else if (qtyStr.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                                channelQuantity = "Voltage";
                                            else if (qtyStr.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                                channelQuantity = "Power";
                                            else
                                                channelQuantity = qtyStr;
                                                
                                            if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0)
                                            {
                                                DebugWriteLine(debug, $"    Found quantity from ChannelDefinition (via SeriesDefinition): '{qtyStr}' -> '{channelQuantity}'");
                                            }
                                        }
                                    }
                                    
                                    // Extract phase from ChannelDefinition
                                    if (string.IsNullOrEmpty(channelPhase))
                                    {
                                        var phaseFromChannelDef = GetPropertyValue<object>(channelDefFromSeries, "Phase") ??
                                                                  GetPropertyValue<object>(channelDefFromSeries, "PhaseType") ??
                                                                  GetPropertyValue<object>(channelDefFromSeries, "Connection");
                                        if (phaseFromChannelDef != null)
                                        {
                                            string phaseStr = phaseFromChannelDef.ToString() ?? "";
                                            if (phaseFromChannelDef.GetType().IsEnum)
                                            {
                                                phaseStr = phaseFromChannelDef.GetType().GetEnumName(phaseFromChannelDef) ?? phaseStr;
                                            }
                                            if (phaseStr.Contains("AN", StringComparison.OrdinalIgnoreCase))
                                                channelPhase = "AN";
                                            else if (phaseStr.Contains("BN", StringComparison.OrdinalIgnoreCase))
                                                channelPhase = "BN";
                                            else if (phaseStr.Contains("CN", StringComparison.OrdinalIgnoreCase))
                                                channelPhase = "CN";
                                            else if (phaseStr.Contains("NG", StringComparison.OrdinalIgnoreCase))
                                                channelPhase = "NG";
                                                
                                            if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0 && !string.IsNullOrEmpty(channelPhase))
                                            {
                                                DebugWriteLine(debug, $"    Found Phase from ChannelDefinition (via SeriesDefinition): '{phaseStr}' -> '{channelPhase}'");
                                            }
                                        }
                                    }
                                    
                                    // Also try to get name from ChannelDefinition
                                    if (string.IsNullOrEmpty(channelName))
                                    {
                                        var nameFromChannelDef = GetPropertyValue<string>(channelDefFromSeries, "Name") ??
                                                                 GetPropertyValue<string>(channelDefFromSeries, "Description") ??
                                                                 GetPropertyValue<string>(channelDefFromSeries, "Label");
                                        if (!string.IsNullOrEmpty(nameFromChannelDef))
                                        {
                                            channelName = nameFromChannelDef.Trim();
                                            if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0)
                                            {
                                                DebugWriteLine(debug, $"    Found channel name from ChannelDefinition (via SeriesDefinition): '{channelName}'");
                                            }
                                        }
                                    }
                                }
                                
                                // Try to get quantity from SeriesDefinition
                                if (string.IsNullOrEmpty(channelQuantity))
                                {
                                    var qtyFromSeries = GetPropertyValue<object>(seriesDef, "QuantityMeasured") ??
                                                       GetPropertyValue<object>(seriesDef, "Quantity") ??
                                                       GetPropertyValue<object>(seriesDef, "MeasuredQuantity");
                                    if (qtyFromSeries != null)
                                    {
                                        string qtyStr = qtyFromSeries.ToString() ?? "";
                                        if (qtyFromSeries.GetType().IsEnum)
                                        {
                                            qtyStr = qtyFromSeries.GetType().GetEnumName(qtyFromSeries) ?? qtyStr;
                                        }
                                        if (qtyStr.Contains("Current", StringComparison.OrdinalIgnoreCase))
                                            channelQuantity = "Current";
                                        else if (qtyStr.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                                            channelQuantity = "Voltage";
                                        else if (qtyStr.Contains("Power", StringComparison.OrdinalIgnoreCase))
                                            channelQuantity = "Power";
                                        else
                                            channelQuantity = qtyStr;
                                            
                                        if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0)
                                        {
                                            DebugWriteLine(debug, $"    Found quantity from SeriesDefinition: '{qtyStr}' -> '{channelQuantity}'");
                                        }
                                    }
                                }
                                
                                // Try to get phase from SeriesDefinition
                                if (string.IsNullOrEmpty(channelPhase))
                                {
                                    var phaseFromSeries = GetPropertyValue<object>(seriesDef, "Phase") ??
                                                         GetPropertyValue<object>(seriesDef, "PhaseType") ??
                                                         GetPropertyValue<object>(seriesDef, "Connection");
                                    if (phaseFromSeries != null)
                                    {
                                        string phaseStr = phaseFromSeries.ToString() ?? "";
                                        if (phaseFromSeries.GetType().IsEnum)
                                        {
                                            phaseStr = phaseFromSeries.GetType().GetEnumName(phaseFromSeries) ?? phaseStr;
                                        }
                                        if (phaseStr.Contains("AN", StringComparison.OrdinalIgnoreCase))
                                            channelPhase = "AN";
                                        else if (phaseStr.Contains("BN", StringComparison.OrdinalIgnoreCase))
                                            channelPhase = "BN";
                                        else if (phaseStr.Contains("CN", StringComparison.OrdinalIgnoreCase))
                                            channelPhase = "CN";
                                        else if (phaseStr.Contains("NG", StringComparison.OrdinalIgnoreCase))
                                            channelPhase = "NG";
                                            
                                        if (observationIndex == 0 && channelIndex < 3 && seriesIndex == 0 && !string.IsNullOrEmpty(channelPhase))
                                        {
                                            DebugWriteLine(debug, $"    Found Phase from SeriesDefinition: '{phaseStr}' -> '{channelPhase}'");
                                        }
                                    }
                                }
                            }
                            
                            // Build column name in format: "Quantity Phase (Statistic)" or "Quantity Type (Statistic)"
                            // Examples: "Current NG (RMS)", "Voltage AN (RMS)", "Power Total (S)"
                            
                            // First, try to get statistic/measurement type from SeriesDefinition
                            string statisticType = "";
                            if (seriesDef != null)
                            {
                                // Try ValueTypeName, UnitName, or Name from SeriesDefinition
                                var valueTypeName = GetPropertyValue<string>(seriesDef, "ValueTypeName") ??
                                                   GetPropertyValue<string>(seriesDef, "UnitName") ??
                                                   GetPropertyValue<string>(seriesDef, "Name");
                                
                                if (!string.IsNullOrEmpty(valueTypeName))
                                {
                                    statisticType = valueTypeName;
                                    // Clean up common prefixes/suffixes
                                    statisticType = statisticType.Replace("LN_AVE ", "").Replace("VOLTS", "").Trim();
                                }
                            }
                            
                            // If no statistic type from SeriesDefinition, try SeriesInstance
                            if (string.IsNullOrEmpty(statisticType))
                            {
                                var seriesName = GetPropertyValue<string>(seriesInstance, "Name") ??
                                                GetPropertyValue<string>(seriesInstance, "Description");
                                if (!string.IsNullOrEmpty(seriesName))
                                {
                                    statisticType = seriesName;
                                }
                            }
                            
                            // Common statistic types: RMS, TotalTHD, FlkrPLT, FlkrPST, S, Q, P
                            // If we don't have one, try to infer from common patterns
                            if (string.IsNullOrEmpty(statisticType))
                            {
                                // Pattern: series 0 usually = RMS or base measurement
                                // We can infer common types based on context
                                if (seriesIndex == 0)
                                    statisticType = "RMS"; // Default for first series
                                else
                                    statisticType = ""; // Will be determined from other properties
                            }
                            
                            // Build column name: ChannelName (StatisticType)
                            // ChannelName already contains "Current NG", "Voltage AN", "Power Total", etc.
                            if (!string.IsNullOrEmpty(channelName) && !string.IsNullOrEmpty(statisticType))
                            {
                                columnName = $"{channelName} ({statisticType})";
                            }
                            else if (!string.IsNullOrEmpty(channelName))
                            {
                                // If no statistic type, just use channel name
                                columnName = channelName;
                            }
                            else if (!string.IsNullOrEmpty(statisticType))
                            {
                                // If no channel name, use statistic type with phase
                                if (!string.IsNullOrEmpty(channelPhase))
                                    columnName = $"{statisticType} {channelPhase}";
                                else
                                    columnName = statisticType;
                            }
                            
                            // Fallback: generate it from properties
                            if (string.IsNullOrEmpty(columnName))
                            {
                                // Get statistic type from SeriesDefinition (MAX, MIN, AVG) - reuse existing variable
                                string additionalInfo = "";
                                
                                // If statisticType is still empty, try common pattern: series 0 = MAX, series 1 = MIN, series 2 = AVG
                                if (string.IsNullOrEmpty(statisticType))
                                {
                                    string[] statTypes = { "MAX", "MIN", "AVG" };
                                    if (seriesIndex < statTypes.Length)
                                    {
                                        statisticType = statTypes[seriesIndex];
                                    }
                                }
                            
                                if (seriesDef != null)
                                {
                                // Try ValueTypeID first (we saw this in debug output) - it's a GUID
                                var valueTypeId = GetPropertyValue<object>(seriesDef, "ValueTypeID") ??
                                                 GetPropertyValue<object>(seriesDef, "SeriesValueTypeID");
                                if (valueTypeId != null)
                                {
                                    string valTypeGuid = valueTypeId.ToString() ?? "";
                                    
                                    // Known PQDIF ValueTypeID GUIDs (from IEEE 1159.3 standard)
                                    // These are standard GUIDs for Maximum, Minimum, Average value types
                                    if (valTypeGuid.Contains("f76e") || valTypeGuid.Contains("3d786f9e"))
                                    {
                                        // Check specific GUIDs - these are approximations, exact GUIDs need to be looked up
                                        // For now, try to infer from context or use series index pattern
                                    }
                                    
                                    // Try to get name/description from ValueTypeID if library provides it
                                    // This would require helper methods from the library
                                }
                                
                                // Check for additional descriptors like "LN_AVE VOLTS"
                                var valueTypeName = GetPropertyValue<string>(seriesDef, "ValueTypeName");
                                if (string.IsNullOrEmpty(valueTypeName))
                                {
                                    valueTypeName = GetPropertyValue<string>(seriesDef, "UnitName");
                                    if (string.IsNullOrEmpty(valueTypeName))
                                        valueTypeName = GetPropertyValue<string>(seriesDef, "Unit");
                                }
                                
                                if (!string.IsNullOrEmpty(valueTypeName))
                                {
                                    if (valueTypeName.Contains("LN_AVE") || valueTypeName.Contains("LineToNeutral"))
                                        additionalInfo = " LN_AVE";
                                    if (valueTypeName.Contains("VOLTS") || valueTypeName.Contains("Volts"))
                                        additionalInfo += " VOLTS";
                                }
                            }
                            
                            // Use channel quantity or try series quantity with mapping
                            string quantityType = channelQuantity;
                            if (string.IsNullOrEmpty(quantityType) && seriesDef != null)
                            {
                                // Try to get quantity from ChannelDefinition through SeriesDefinition
                                var seriesChannelDef = GetPropertyValue<object>(seriesDef, "ChannelDefinition");
                                if (seriesChannelDef != null)
                                {
                                    var seriesChannelQty = GetPropertyValue<object>(seriesChannelDef, "QuantityMeasured");
                                    if (seriesChannelQty != null)
                                    {
                                        string qtyStr = seriesChannelQty.ToString() ?? "";
                                        if (seriesChannelQty.GetType().IsEnum)
                                        {
                                            qtyStr = seriesChannelQty.GetType().GetEnumName(seriesChannelQty) ?? qtyStr;
                                        }
                                        // Apply mapping to the value
                                        quantityType = MapQuantityType(qtyStr);
                                    }
                                }
                                
                                // Try series quantity properties
                                if (string.IsNullOrEmpty(quantityType))
                                {
                                    var seriesQty = GetPropertyValue<object>(seriesDef, "QuantityMeasured") ??
                                                   GetPropertyValue<object>(seriesDef, "QuantityName");
                                    if (seriesQty != null)
                                    {
                                        string qtyStr = seriesQty.ToString() ?? "";
                                        if (seriesQty.GetType().IsEnum)
                                        {
                                            qtyStr = seriesQty.GetType().GetEnumName(seriesQty) ?? qtyStr;
                                        }
                                        // Apply mapping to the value
                                        quantityType = MapQuantityType(qtyStr);
                                    }
                                }
                            }
                            
                            // If still empty, use channelQuantity which should be set
                            if (string.IsNullOrEmpty(quantityType))
                            {
                                quantityType = channelQuantity;
                            }
                            
                            // Build column name with hierarchy: STAT[QUANTITY][PHASE]
                            // e.g., MAX[RMS][AN], MIN[RMS][BN], AVG[RMS][CN]
                            if (!string.IsNullOrEmpty(statisticType) && !string.IsNullOrEmpty(quantityType))
                            {
                                if (!string.IsNullOrEmpty(channelPhase))
                                {
                                    columnName = $"{statisticType}[{quantityType}][{channelPhase}{additionalInfo}]";
                                }
                                else
                                {
                                    columnName = $"{statisticType}[{quantityType}{additionalInfo}]";
                                }
                            }
                            else if (!string.IsNullOrEmpty(quantityType))
                            {
                                if (!string.IsNullOrEmpty(channelPhase))
                                {
                                    columnName = $"{quantityType}[{channelPhase}{additionalInfo}]";
                                }
                                else
                                {
                                    columnName = quantityType + additionalInfo;
                                }
                            }
                            else if (!string.IsNullOrEmpty(statisticType))
                            {
                                if (!string.IsNullOrEmpty(channelPhase))
                                {
                                    columnName = $"{statisticType}[{channelPhase}]";
                                }
                                else
                                {
                                    columnName = statisticType;
                                }
                                }
                                
                                // Fallback to generic name if still empty
                                if (string.IsNullOrEmpty(columnName))
                                {
                                    columnName = $"Channel_{channelIndex}_Series_{seriesIndex}";
                                }
                            }
                            
                            allColumns.Add(columnName);
                            
                            // Track if this column has meaningful values (will be checked after processing)

                            // Get values - use SeriesValues (VectorElement) GetValues() method for full data
                            // Variables already declared above
                            if (seriesValuesObj == null)
                                seriesValuesObj = GetPropertyValue<object>(seriesInstance, "SeriesValues");
                            
                            // Try to call GetValues() on VectorElement to get all the data
                            if (seriesValuesObj != null)
                            {
                                Type vectorType = seriesValuesObj.GetType();
                                
                                // First check Size property to see how many vectors are there
                                var sizeProp = vectorType.GetProperty("Size");
                                int? expectedSize = null;
                                if (sizeProp != null)
                                {
                                    try
                                    {
                                        var sizeVal = sizeProp.GetValue(seriesValuesObj);
                                        if (sizeVal is int sizeInt)
                                        {
                                            expectedSize = sizeInt;
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    VectorElement.Size: {sizeInt} (number of vectors)");
                                            }
                                        }
                                    }
                                    catch { }
                                }
                                
                                // Iterate through ALL vector elements (0, 1, 2, ...) to get all values
                                // User said we're not reading all vector elements - need to check all of them
                                if (expectedSize.HasValue && expectedSize.Value > 0)
                                {
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    Trying to access all VectorElements (Size={expectedSize.Value})...");
                                    }
                                    
                                    // Use Get(Int32) method to access ALL vector elements, not just [0]
                                    // Iterate through all vector elements from 0 to expectedSize-1
                                    var getMethod = vectorType.GetMethod("Get", new[] { typeof(int) });
                                    if (getMethod != null)
                                    {
                                        try
                                        {
                                            List<object?> allVectorValues = new List<object?>();
                                            
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    Accessing all {expectedSize.Value} VectorElements using Get()...");
                                            }
                                            
                                            // Iterate through all vector elements
                                            for (int vectorIndex = 0; vectorIndex < expectedSize.Value; vectorIndex++)
                                            {
                                                var vectorElement = getMethod.Invoke(seriesValuesObj, new object[] { vectorIndex });
                                                
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && vectorIndex < 3)
                                                {
                                                    DebugWriteLine(debug, $"    VectorElement[{vectorIndex}] type: {vectorElement?.GetType().Name ?? "null"}");
                                                }
                                                
                                                // Check if vectorElement is a collection/list of values
                                                if (vectorElement != null && vectorElement is System.Collections.IEnumerable vectorEnum && !(vectorElement is string))
                                                {
                                                    foreach (var val in vectorEnum)
                                                    {
                                                        allVectorValues.Add(val);
                                                    }
                                                    
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && vectorIndex < 3)
                                                    {
                                                        int count = vectorEnum.Cast<object?>().Count();
                                                        DebugWriteLine(debug, $"    VectorElement[{vectorIndex}] contains {count} values");
                                                    }
                                                }
                                                else if (vectorElement != null)
                                                {
                                                    // Single value, not a collection
                                                    allVectorValues.Add(vectorElement);
                                                }
                                            }
                                            
                                            if (allVectorValues.Count > valueCount)
                                            {
                                                values = allVectorValues;
                                                valueCount = allVectorValues.Count;
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"    *** Found {allVectorValues.Count} total values from all {expectedSize.Value} VectorElements! ***");
                                                }
                                            }
                                            
                                            // Also check vector[0] for backwards compatibility
                                            var vector0 = getMethod.Invoke(seriesValuesObj, new object[] { 0 });
                                            
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    VectorElement.Get(0) returned: {vector0} (type: {vector0?.GetType().Name ?? "null"})");
                                            }
                                            
                                            // User suggests trying "the physics value" or "the physical value"
                                            // Let's check for physical/physics related properties or methods
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    Checking VectorElement for physical/physics values:");
                                                // Check all properties that might contain physical values
                                                foreach (var prop in vectorType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic))
                                                {
                                                    try
                                                    {
                                                        var propVal = prop.GetValue(seriesValuesObj);
                                                        if (propVal != null)
                                                        {
                                                            // Check if property name contains "Physical", "Physics", "Value", "Data"
                                                            if (prop.Name.Contains("Physical", StringComparison.OrdinalIgnoreCase) ||
                                                                prop.Name.Contains("Physics", StringComparison.OrdinalIgnoreCase) ||
                                                                prop.Name.Contains("Value", StringComparison.OrdinalIgnoreCase) ||
                                                                prop.Name.Contains("Data", StringComparison.OrdinalIgnoreCase))
                                                            {
                                                                DebugWriteLine(debug, $"      VectorElement.{prop.Name}: {propVal.GetType().Name}");
                                                                
                                                                if (propVal is System.Collections.IEnumerable propEnum && !(propVal is string))
                                                                {
                                                                    int propCount = 0;
                                                                    List<object?> propValues = new List<object?>();
                                                                    foreach (var item in propEnum) 
                                                                    { 
                                                                        propCount++; 
                                                                        propValues.Add(item);
                                                                        if (propCount > 10) break; 
                                                                    }
                                                                    DebugWriteLine(debug, $"        Count: {propCount}");
                                                                    
                                                                    if (propCount > 50 && propCount > valueCount)
                                                                    {
                                                                        // Get full count
                                                                        propCount = propEnum.Cast<object?>().Count();
                                                                        values = propEnum.Cast<object?>();
                                                                        valueCount = propCount;
                                                                        DebugWriteLine(debug, $"        *** Found {propCount} values in VectorElement.{prop.Name}! ***");
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    catch { }
                                                }
                                                
                                                // Check TypeOfValue (PhysicalType) for methods to get physical values
                                                // Get TypeOfValue property first
                                                var typeOfValuePropForPhysical = vectorType.GetProperty("TypeOfValue");
                                                if (typeOfValuePropForPhysical != null)
                                                {
                                                    var physicalType = typeOfValuePropForPhysical.GetValue(seriesValuesObj);
                                                    if (physicalType != null)
                                                    {
                                                        Type physType = physicalType.GetType();
                                                        DebugWriteLine(debug, $"    Checking PhysicalType ({physType.Name}) for value access methods:");
                                                        
                                                        foreach (var method in physType.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static))
                                                        {
                                                            if (method.Name.Contains("Get") || method.Name.Contains("Parse") || method.Name.Contains("Convert"))
                                                            {
                                                                var paramTypes = method.GetParameters().Select(p => p.ParameterType.Name);
                                                                DebugWriteLine(debug, $"      PhysicalType.{method.Name}({string.Join(", ", paramTypes)})");
                                                            }
                                                        }
                                                    }
                                                    
                                                // Expand physical values by decoding the Values byte array based on TypeOfValue
                                                var valuesProp = vectorType.GetProperty("Values");
                                                if (valuesProp != null)
                                                {
                                                    var byteArray = valuesProp.GetValue(seriesValuesObj) as byte[];
                                                    if (byteArray != null && byteArray.Length > 0)
                                                    {
                                                        var typeOfValueForDecode = typeOfValuePropForPhysical.GetValue(seriesValuesObj);
                                                        string typeOfValueStrForDecode = typeOfValueForDecode?.ToString() ?? "unknown";
                                                        
                                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                        {
                                                            DebugWriteLine(debug, $"    Expanding physical values from {byteArray.Length} bytes, TypeOfValue: {typeOfValueStrForDecode}");
                                                        }
                                                        
                                                        // Decode bytes based on physical type
                                                        List<object?> decodedValues = new List<object?>();
                                                        
                                                        try
                                                        {
                                                            // Parse bytes based on TypeOfValue
                                                            if (typeOfValueStrForDecode.Contains("Integer4") || typeOfValueStrForDecode.Contains("Int4"))
                                                            {
                                                                // Integer4 = 4 bytes per value
                                                                int bytesPerValue = 4;
                                                                for (int i = 0; i < byteArray.Length; i += bytesPerValue)
                                                                {
                                                                    if (i + bytesPerValue <= byteArray.Length)
                                                                    {
                                                                        int value = BitConverter.ToInt32(byteArray, i);
                                                                        decodedValues.Add(value);
                                                                    }
                                                                }
                                                            }
                                                            else if (typeOfValueStrForDecode.Contains("Real8") || typeOfValueStrForDecode.Contains("Double"))
                                                            {
                                                                // Real8 = 8 bytes per value
                                                                int bytesPerValue = 8;
                                                                for (int i = 0; i < byteArray.Length; i += bytesPerValue)
                                                                {
                                                                    if (i + bytesPerValue <= byteArray.Length)
                                                                    {
                                                                        double value = BitConverter.ToDouble(byteArray, i);
                                                                        decodedValues.Add(value);
                                                                    }
                                                                }
                                                            }
                                                            else if (typeOfValueStrForDecode.Contains("Real4") || typeOfValueStrForDecode.Contains("Single") || typeOfValueStrForDecode.Contains("Float"))
                                                            {
                                                                // Real4 = 4 bytes per value
                                                                int bytesPerValue = 4;
                                                                for (int i = 0; i < byteArray.Length; i += bytesPerValue)
                                                                {
                                                                    if (i + bytesPerValue <= byteArray.Length)
                                                                    {
                                                                        float value = BitConverter.ToSingle(byteArray, i);
                                                                        decodedValues.Add(value);
                                                                    }
                                                                }
                                                            }
                                                            
                                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                            {
                                                                DebugWriteLine(debug, $"    Decoded {decodedValues.Count} values from byte array");
                                                                if (decodedValues.Count > 0 && decodedValues.Count <= 5)
                                                                {
                                                                    foreach (var val in decodedValues)
                                                                    {
                                                                        DebugWriteLine(debug, $"      Decoded value: {val}");
                                                                    }
                                                                }
                                                            }
                                                            
                                                            // Store decoded values for later expansion with counts
                                                            if (decodedValues.Count > valueCount)
                                                            {
                                                                // Use decoded values directly if no counts to expand
                                                                values = decodedValues;
                                                                valueCount = decodedValues.Count;
                                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                                {
                                                                    DebugWriteLine(debug, $"    *** Using {decodedValues.Count} decoded physical values! ***");
                                                                }
                                                            }
                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                            {
                                                            DebugWriteLine(debug, $"    Error decoding physical values: {ex.Message}");
                                                            }
                                                        }
                                                    }
                                                }
                                                }
                                            }
                                            
                                            // Check if vector0 is a collection/list of values
                                            if (vector0 != null && vector0 is System.Collections.IEnumerable vector0Enum && !(vector0 is string))
                                            {
                                                List<object?> vector0Values = new List<object?>();
                                                int vector0Count = 0;
                                                
                                                foreach (var val in vector0Enum)
                                                {
                                                    vector0Values.Add(val);
                                                    vector0Count++;
                                                    
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && vector0Count <= 5)
                                                    {
                                                        DebugWriteLine(debug, $"      VectorElement[0][{vector0Count - 1}]: {val} (type: {val?.GetType().Name ?? "null"})");
                                                    }
                                                }
                                                
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"    VectorElement[0] contains {vector0Count} values");
                                                }
                                                
                                                if (vector0Count > valueCount)
                                                {
                                                    values = vector0Values;
                                                    valueCount = vector0Count;
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                    {
                                                        DebugWriteLine(debug, $"    *** Found {vector0Count} values in VectorElement[0] using Get(0)! ***");
                                                    }
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    Error accessing VectorElement[0] via Get(0): {ex.Message}");
                                            }
                                        }
                                    }
                                    
                                    try
                                    {
                                        // Try indexer property
                                        var indexerProp = vectorType.GetProperty("Item");
                                        if (indexerProp != null)
                                        {
                                            List<object?> allVectorValuesFromIndexer = new List<object?>();
                                            
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    Found Item property (indexer), accessing all {expectedSize.Value} VectorElements...");
                                            }
                                            
                                            // Iterate through all vector elements using indexer
                                            for (int vectorIndex = 0; vectorIndex < expectedSize.Value; vectorIndex++)
                                            {
                                                var vectorElement = indexerProp.GetValue(seriesValuesObj, new object[] { vectorIndex });
                                                
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && vectorIndex < 3)
                                                {
                                                    DebugWriteLine(debug, $"    VectorElement[{vectorIndex}] type: {vectorElement?.GetType().Name ?? "null"}");
                                                }
                                                
                                                // Check if vectorElement is a collection/list of values
                                                if (vectorElement != null && vectorElement is System.Collections.IEnumerable vectorEnum && !(vectorElement is string))
                                                {
                                                    foreach (var val in vectorEnum)
                                                    {
                                                        allVectorValuesFromIndexer.Add(val);
                                                    }
                                                    
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && vectorIndex < 3)
                                                    {
                                                        int count = vectorEnum.Cast<object?>().Count();
                                                        DebugWriteLine(debug, $"    VectorElement[{vectorIndex}] contains {count} values");
                                                    }
                                                }
                                                else if (vectorElement != null)
                                                {
                                                    // Single value, not a collection
                                                    allVectorValuesFromIndexer.Add(vectorElement);
                                                }
                                            }
                                            
                                            if (allVectorValuesFromIndexer.Count > valueCount)
                                            {
                                                values = allVectorValuesFromIndexer;
                                                valueCount = allVectorValuesFromIndexer.Count;
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"    *** Found {allVectorValuesFromIndexer.Count} total values from all {expectedSize.Value} VectorElements using indexer! ***");
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"    Error accessing VectorElement[0] via indexer: {ex.Message}");
                                        }
                                    }
                                    
                                    // Also try GetElement() or similar methods
                                    var getElementMethod = vectorType.GetMethod("GetElement");
                                    if (getElementMethod != null)
                                    {
                                        try
                                        {
                                            var vector0 = getElementMethod.Invoke(seriesValuesObj, new object[] { 0 });
                                            
                                            if (vector0 != null && vector0 is System.Collections.IEnumerable vector0Enum && !(vector0 is string))
                                            {
                                                List<object?> vector0Values = new List<object?>();
                                                int vector0Count = 0;
                                                
                                                foreach (var val in vector0Enum)
                                                {
                                                    vector0Values.Add(val);
                                                    vector0Count++;
                                                }
                                                
                                                if (vector0Count > valueCount && vector0Count > 50)
                                                {
                                                    values = vector0Values;
                                                    valueCount = vector0Count;
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                    {
                                                        DebugWriteLine(debug, $"    *** Found {vector0Count} values using GetElement(0)! ***");
                                                    }
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                }
                                
                                // Check TypeOfValue to determine how to access values
                                var typeOfValueProp = vectorType.GetProperty("TypeOfValue");
                                object? typeOfValueObj = null;
                                string typeOfValueStr = "";
                                if (typeOfValueProp != null)
                                {
                                    typeOfValueObj = typeOfValueProp.GetValue(seriesValuesObj);
                                    typeOfValueStr = typeOfValueObj?.ToString() ?? "";
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    VectorElement.TypeOfValue: {typeOfValueStr}");
                                    }
                                }
                                
                                // Try accessing values using physical methods (GetReal8, GetInt4, etc.) with larger indices
                                // Sometimes Size is wrong or represents chunks, so try accessing up to a large number
                                if (expectedSize.HasValue && expectedSize.Value == 1)
                                {
                                    // Size says 1, but we might have more values - try accessing directly
                                    List<object?> directValues = new List<object?>();
                                    for (int testIndex = 0; testIndex < 10000; testIndex++)
                                    {
                                        try
                                        {
                                            object? testVal = null;
                                            if (typeOfValueStr.Contains("Real8") || typeOfValueStr.Contains("Double"))
                                            {
                                                var getReal8Method = vectorType.GetMethod("GetReal8", new[] { typeof(int) });
                                                if (getReal8Method != null)
                                                {
                                                    testVal = getReal8Method.Invoke(seriesValuesObj, new object[] { testIndex });
                                                }
                                            }
                                            else if (typeOfValueStr.Contains("Real4") || typeOfValueStr.Contains("Single") || typeOfValueStr.Contains("Float"))
                                            {
                                                var getReal4Method = vectorType.GetMethod("GetReal4", new[] { typeof(int) });
                                                if (getReal4Method != null)
                                                {
                                                    testVal = getReal4Method.Invoke(seriesValuesObj, new object[] { testIndex });
                                                }
                                            }
                                            else if (typeOfValueStr.Contains("Int4") || typeOfValueStr.Contains("Integer4"))
                                            {
                                                var getInt4Method = vectorType.GetMethod("GetInt4", new[] { typeof(int) });
                                                if (getInt4Method != null)
                                                {
                                                    testVal = getInt4Method.Invoke(seriesValuesObj, new object[] { testIndex });
                                                }
                                            }
                                            
                                            if (testVal != null)
                                            {
                                                directValues.Add(testVal);
                                                if (testIndex == 0 && observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"    Direct access works! Trying up to 10000 indices...");
                                                }
                                            }
                                            else
                                            {
                                                // No more values at this index
                                                if (directValues.Count > 0 && testIndex > directValues.Count * 2)
                                                {
                                                    // Stop if we've gone significantly past where we found values
                                                    break;
                                                }
                                            }
                                        }
                                        catch
                                        {
                                            // Index out of bounds or error - stop
                                            break;
                                        }
                                        
                                        // Stop if we've found a reasonable number
                                        if (directValues.Count >= 100 && testIndex > directValues.Count)
                                        {
                                            break;
                                        }
                                    }
                                    
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    Direct access found {directValues.Count} values");
                                    }
                                    
                                    if (directValues.Count > valueCount)
                                    {
                                        values = directValues;
                                        valueCount = directValues.Count;
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"    *** Using {valueCount} values from direct physical access! ***");
                                        }
                                    }
                                }
                                
                                // Call GetValues() method on VectorElement to get counts
                                // According to user: GetValues() returns counts, not actual values
                                // We need to use these counts to access actual values
                                var getValuesMethods = vectorType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                                    .Where(m => m.Name == "GetValues").ToList();
                                
                                // First get the counts from GetValues()
                                List<int> countsFromGetValues = new List<int>();
                                if (getValuesMethods.Count > 0)
                                {
                                    var getValuesMethod = getValuesMethods[0]; // Use parameterless version
                                    if (getValuesMethod.GetParameters().Length == 0)
                                    {
                                        var countsResult = getValuesMethod.Invoke(seriesValuesObj, null);
                                        if (countsResult != null && countsResult is System.Collections.IEnumerable countsEnum)
                                        {
                                            foreach (var countObj in countsEnum)
                                            {
                                                if (countObj is int intCount)
                                                    countsFromGetValues.Add(intCount);
                                                else if (countObj is long longCount && longCount <= int.MaxValue)
                                                    countsFromGetValues.Add((int)longCount);
                                                else if (countObj != null)
                                                {
                                                    // Try to convert
                                                    if (int.TryParse(countObj.ToString(), out int parsedCount))
                                                        countsFromGetValues.Add(parsedCount);
                                                }
                                            }
                                            
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    GetValues() returned counts: {string.Join(", ", countsFromGetValues)}");
                                                DebugWriteLine(debug, $"    Total count sum: {countsFromGetValues.Sum()}");
                                            }
                                        }
                                    }
                                }
                                
                                // Expand physical values by decoding VectorElement.Values byte array and using counts
                                List<object?> decodedValuesFromBytes = new List<object?>();
                                var valuesPropForExpand = vectorType.GetProperty("Values");
                                if (valuesPropForExpand != null)
                                {
                                    var byteArray = valuesPropForExpand.GetValue(seriesValuesObj) as byte[];
                                    if (byteArray != null && byteArray.Length > 0)
                                    {
                                        // Get TypeOfValue to decode bytes correctly
                                        var typeOfValuePropForDecode = vectorType.GetProperty("TypeOfValue");
                                        if (typeOfValuePropForDecode != null)
                                        {
                                            var typeOfValueForDecode = typeOfValuePropForDecode.GetValue(seriesValuesObj);
                                            string typeOfValueStrForDecode = typeOfValueForDecode?.ToString() ?? "unknown";
                                            
                                            // Decode bytes based on physical type
                                            try
                                            {
                                                if (typeOfValueStrForDecode.Contains("Integer4") || typeOfValueStrForDecode.Contains("Int4"))
                                                {
                                                    int bytesPerValue = 4;
                                                    for (int i = 0; i < byteArray.Length; i += bytesPerValue)
                                                    {
                                                        if (i + bytesPerValue <= byteArray.Length)
                                                        {
                                                            int value = BitConverter.ToInt32(byteArray, i);
                                                            decodedValuesFromBytes.Add(value);
                                                        }
                                                    }
                                                }
                                                else if (typeOfValueStrForDecode.Contains("Real8") || typeOfValueStrForDecode.Contains("Double"))
                                                {
                                                    int bytesPerValue = 8;
                                                    for (int i = 0; i < byteArray.Length; i += bytesPerValue)
                                                    {
                                                        if (i + bytesPerValue <= byteArray.Length)
                                                        {
                                                            double value = BitConverter.ToDouble(byteArray, i);
                                                            decodedValuesFromBytes.Add(value);
                                                        }
                                                    }
                                                }
                                                else if (typeOfValueStrForDecode.Contains("Real4") || typeOfValueStrForDecode.Contains("Single") || typeOfValueStrForDecode.Contains("Float"))
                                                {
                                                    int bytesPerValue = 4;
                                                    for (int i = 0; i < byteArray.Length; i += bytesPerValue)
                                                    {
                                                        if (i + bytesPerValue <= byteArray.Length)
                                                        {
                                                            float value = BitConverter.ToSingle(byteArray, i);
                                                            decodedValuesFromBytes.Add(value);
                                                        }
                                                    }
                                                }
                                                
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && decodedValuesFromBytes.Count > 0)
                                                {
                                                    DebugWriteLine(debug, $"    Decoded {decodedValuesFromBytes.Count} physical values from {byteArray.Length} bytes (TypeOfValue: {typeOfValueStrForDecode})");
                                                }
                                                
                                                // Use decoded values directly if we have enough (prioritize decoded values)
                                                if (decodedValuesFromBytes.Count > valueCount)
                                                {
                                                    values = decodedValuesFromBytes;
                                                    valueCount = decodedValuesFromBytes.Count;
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                    {
                                                        DebugWriteLine(debug, $"    *** Using {decodedValuesFromBytes.Count} decoded physical values from byte array! ***");
                                                    }
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"    Error decoding bytes: {ex.Message}");
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                // Try interpreting GetValues() results as encoded physical values (not counts)
                                // The values from GetValues() might be the actual data encoded as bytes
                                if (countsFromGetValues.Count > 0 && countsFromGetValues.Count > valueCount)
                                {
                                    // Check if these look like encoded double values (8 bytes each)
                                    // Pattern: groups of 8 bytes that could be IEEE 754 doubles
                                    List<object?> decodedFromGetValues = new List<object?>();
                                    
                                    // Try to decode as doubles (8 bytes per value)
                                    byte[] getValuesBytes = new byte[countsFromGetValues.Count];
                                    for (int i = 0; i < countsFromGetValues.Count && i < 255; i++)
                                    {
                                        getValuesBytes[i] = (byte)(countsFromGetValues[i] & 0xFF);
                                    }
                                    
                                    // Decode based on TypeOfValue
                                    if (typeOfValueStr.Contains("Real8") || typeOfValueStr.Contains("Double"))
                                    {
                                        for (int i = 0; i + 8 <= getValuesBytes.Length; i += 8)
                                        {
                                            try
                                            {
                                                double value = BitConverter.ToDouble(getValuesBytes, i);
                                                decodedFromGetValues.Add(value);
                                            }
                                            catch { break; }
                                        }
                                        
                                        if (decodedFromGetValues.Count > valueCount)
                                        {
                                            values = decodedFromGetValues;
                                            valueCount = decodedFromGetValues.Count;
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    *** Decoded {decodedFromGetValues.Count} physical values from GetValues() (Real8)! ***");
                                            }
                                        }
                                    }
                                    else if (typeOfValueStr.Contains("Integer4") || typeOfValueStr.Contains("Int4"))
                                    {
                                        // For Integer4, decode as 4-byte integers
                                        for (int i = 0; i + 4 <= getValuesBytes.Length; i += 4)
                                        {
                                            try
                                            {
                                                int value = BitConverter.ToInt32(getValuesBytes, i);
                                                decodedFromGetValues.Add(value);
                                            }
                                            catch { break; }
                                        }
                                        
                                        if (decodedFromGetValues.Count > valueCount)
                                        {
                                            values = decodedFromGetValues;
                                            valueCount = decodedFromGetValues.Count;
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    *** Decoded {decodedFromGetValues.Count} physical values from GetValues() (Integer4)! ***");
                                            }
                                        }
                                    }
                                    else if (typeOfValueStr.Contains("Real4") || typeOfValueStr.Contains("Single") || typeOfValueStr.Contains("Float"))
                                    {
                                        // For Real4, decode as 4-byte floats
                                        for (int i = 0; i + 4 <= getValuesBytes.Length; i += 4)
                                        {
                                            try
                                            {
                                                float value = BitConverter.ToSingle(getValuesBytes, i);
                                                decodedFromGetValues.Add(value);
                                            }
                                            catch { break; }
                                        }
                                        
                                        if (decodedFromGetValues.Count > valueCount)
                                        {
                                            values = decodedFromGetValues;
                                            valueCount = decodedFromGetValues.Count;
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                                DebugWriteLine(debug, $"    *** Decoded {decodedFromGetValues.Count} physical values from GetValues() (Real4)! ***");
                                            }
                                        }
                                    }
                                }
                                
                                // Expand decoded values using counts from GetValues()
                                if (countsFromGetValues.Count > 0 && decodedValuesFromBytes.Count > 0)
                                {
                                    int totalCountSum = countsFromGetValues.Sum();
                                    
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    Expanding {decodedValuesFromBytes.Count} decoded values using counts: {string.Join(", ", countsFromGetValues.Take(10))}... (sum: {totalCountSum})");
                                    }
                                    
                                    // Expand decoded values based on counts
                                    List<object?> expandedValues = new List<object?>();
                                    int decodedIndex = 0;
                                    
                                    foreach (var count in countsFromGetValues)
                                    {
                                        // If count is 0, skip
                                        if (count == 0)
                                            continue;
                                        
                                        // For each count, use the corresponding decoded value
                                        if (decodedIndex < decodedValuesFromBytes.Count)
                                        {
                                            // Repeat the decoded value 'count' times
                                            for (int i = 0; i < count; i++)
                                            {
                                                expandedValues.Add(decodedValuesFromBytes[decodedIndex]);
                                            }
                                            decodedIndex++;
                                        }
                                        else
                                        {
                                            // If we run out of decoded values, repeat the last one
                                            if (decodedValuesFromBytes.Count > 0)
                                            {
                                                for (int i = 0; i < count; i++)
                                                {
                                                    expandedValues.Add(decodedValuesFromBytes[decodedValuesFromBytes.Count - 1]);
                                                }
                                            }
                                        }
                                    }
                                    
                                    if (expandedValues.Count > valueCount)
                                    {
                                        values = expandedValues;
                                        valueCount = expandedValues.Count;
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"    *** Expanded to {expandedValues.Count} physical values! ***");
                                        }
                                    }
                                }
                                
                                // Now use these counts to read actual values using indexed access methods
                                // Try GetReal8(index), GetInt4(index), etc. to read actual values
                                // The counts tell us how many values to read - skip 0 counts
                                if (countsFromGetValues.Count > 0 && countsFromGetValues.Sum() > valueCount)
                                {
                                    int totalCountSum = countsFromGetValues.Sum();
                                    
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    Attempting to read {totalCountSum} actual values using counts...");
                                    }
                                    
                                    List<object?> actualValuesFromCounts = new List<object?>();
                                    int currentIndex = 0;
                                    int maxReadAttempts = Math.Min(totalCountSum, 10000); // Limit to prevent infinite loops
                                    
                                    // Read all values sequentially based on the counts
                                    // Each non-zero count tells us how many values to read at that position
                                    foreach (var count in countsFromGetValues)
                                    {
                                        // Skip 0 counts (no data at this position)
                                        if (count == 0)
                                            continue;
                                        
                                        // Read 'count' number of actual values starting from currentIndex
                                        for (int i = 0; i < count && actualValuesFromCounts.Count < maxReadAttempts; i++)
                                        {
                                            try
                                            {
                                                object? actualVal = null;
                                                
                                                // Try to get actual value using indexed access
                                                if (typeOfValueStr.Contains("Real8") || typeOfValueStr.Contains("Double"))
                                                {
                                                    var getReal8Method = vectorType.GetMethod("GetReal8", new[] { typeof(int) });
                                                    if (getReal8Method != null)
                                                    {
                                                        actualVal = getReal8Method.Invoke(seriesValuesObj, new object[] { currentIndex });
                                                    }
                                                }
                                                else if (typeOfValueStr.Contains("Real4") || typeOfValueStr.Contains("Single") || typeOfValueStr.Contains("Float"))
                                                {
                                                    var getReal4Method = vectorType.GetMethod("GetReal4", new[] { typeof(int) });
                                                    if (getReal4Method != null)
                                                    {
                                                        actualVal = getReal4Method.Invoke(seriesValuesObj, new object[] { currentIndex });
                                                    }
                                                }
                                                else if (typeOfValueStr.Contains("Int4") || typeOfValueStr.Contains("Integer4"))
                                                {
                                                    var getInt4Method = vectorType.GetMethod("GetInt4", new[] { typeof(int) });
                                                    if (getInt4Method != null)
                                                    {
                                                        actualVal = getInt4Method.Invoke(seriesValuesObj, new object[] { currentIndex });
                                                    }
                                                }
                                                
                                                if (actualVal != null)
                                                {
                                                    actualValuesFromCounts.Add(actualVal);
                                                    currentIndex++;
                                                    
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && actualValuesFromCounts.Count <= 5)
                                                    {
                                                        DebugWriteLine(debug, $"      Actual value[{actualValuesFromCounts.Count - 1}]: {actualVal} (index: {currentIndex - 1})");
                                                    }
                                                }
                                                else
                                                {
                                                    // No more values at this index - stop
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                    {
                                                        DebugWriteLine(debug, $"      Stopped: null value at index {currentIndex}");
                                                    }
                                                    break;
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                // Index out of bounds or error - stop reading
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && actualValuesFromCounts.Count < 10)
                                                {
                                                DebugWriteLine(debug, $"      Stopped reading at index {currentIndex}: {ex.Message}");
                                                }
                                                break;
                                            }
                                        }
                                    }
                                    
                                    if (actualValuesFromCounts.Count > valueCount)
                                    {
                                        values = actualValuesFromCounts;
                                        valueCount = actualValuesFromCounts.Count;
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"    *** Successfully read {actualValuesFromCounts.Count} actual values (expected {totalCountSum}) using counts from GetValues()! ***");
                                        }
                                    }
                                    else if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    Only read {actualValuesFromCounts.Count} values (expected {totalCountSum}) - may need different approach");
                                    }
                                }
                                
                                // Check if we have decoded physical values (doubles/floats) - don't overwrite them!
                                bool hasDecodedPhysicalValues = values != null && values.Cast<object?>().Any(v => 
                                    v is double || v is float || (v is int intVal && intVal > 255));
                                
                                // First, check if there's a way to get all values - maybe via a property or method
                                // Check for properties that might contain all the data
                                // BUT: Don't overwrite decoded physical values (they should take priority)
                                if (!hasDecodedPhysicalValues && (valueCount < 10 || (values != null && values.Cast<object?>().Any(v => v is byte || (v is int intVal && intVal >= 0 && intVal <= 255)))))
                                {
                                    // Only look for other sources if we don't have good decoded values
                                    foreach (var prop in vectorType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                    {
                                        if (prop.Name.Contains("Data") || prop.Name.Contains("Array") || prop.Name.Contains("Buffer"))
                                        {
                                            try
                                            {
                                                var propVal = prop.GetValue(seriesValuesObj);
                                                if (propVal != null && propVal is System.Collections.IEnumerable propEnum)
                                                {
                                                    int propCount = propEnum.Cast<object?>().Count();
                                                    if (propCount > valueCount && propCount > 50) // Only if it looks like real data
                                                    {
                                                        // Check if prop values are actually physical values (doubles/floats)
                                                        var firstVal = propEnum.Cast<object?>().FirstOrDefault();
                                                        bool propHasPhysicalValues = firstVal is double || firstVal is float || 
                                                            (firstVal is int intPropVal && intPropVal > 255);
                                                        
                                                        if (propHasPhysicalValues || !hasDecodedPhysicalValues)
                                                        {
                                                            values = propEnum.Cast<object?>();
                                                            valueCount = propCount;
                                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                            {
                                                                DebugWriteLine(debug, $"    Found {prop.Name} with {propCount} values!");
                                                            }
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            catch { }
                                        }
                                    }
                                }
                                
                                // If we didn't find data in properties, try GetValues() methods with various parameters
                                // BUT: Don't overwrite decoded physical values!
                                bool stillHasByteValues = values != null && values.Cast<object?>().Any(v => v is byte || (v is int intVal && intVal >= 0 && intVal <= 255));
                                if (valueCount < 100 && !hasDecodedPhysicalValues && stillHasByteValues)
                                {
                                    // First, list all GetValues() method signatures
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    Available GetValues() methods:");
                                        foreach (var method in getValuesMethods)
                                        {
                                            var paramTypes = method.GetParameters().Select(p => p.ParameterType.Name);
                                            DebugWriteLine(debug, $"      GetValues({string.Join(", ", paramTypes)})");
                                        }
                                    }
                                    
                                    foreach (var getValuesMethod in getValuesMethods)
                                    {
                                        try
                                        {
                                            object? result = null;
                                            
                                            // Try parameterless version first
                                            // NOTE: User said GetValues() returns counts, not actual values
                                            // We'll use these counts to access actual values via GetReal8, GetInt4, etc.
                                            if (getValuesMethod.GetParameters().Length == 0)
                                            {
                                                result = getValuesMethod.Invoke(seriesValuesObj, null);
                                                
                                                // If result contains counts, use them to get actual values
                                                if (result != null && result is System.Collections.IEnumerable countEnum)
                                                {
                                                    var counts = countEnum.Cast<object?>().ToList();
                                                    
                                                    // Use these counts to access actual values via indexed methods
                                                    if (counts.Count > 0 && counts.Count <= 20) // Reasonable count range
                                                    {
                                                        List<object?> actualValues = new List<object?>();
                                                        int totalActualCount = 0;
                                                        
                                                        // Try to get actual values using the count
                                                        foreach (var countObj in counts)
                                                        {
                                                            int? count = null;
                                                            if (countObj is int intCount)
                                                                count = intCount;
                                                            else if (countObj is long longCount && longCount <= int.MaxValue)
                                                                count = (int)longCount;
                                                            
                                                            if (count.HasValue && count.Value > 0 && count.Value < 10000)
                                                            {
                                                                // Use this count to get actual values - try indexed access methods
                                                                for (int idx = 0; idx < count.Value; idx++)
                                                                {
                                                                    try
                                                                    {
                                                                        object? actualVal = null;
                                                                        if (typeOfValueStr.Contains("Real8") || typeOfValueStr.Contains("Double"))
                                                                        {
                                                                            var getReal8Method = vectorType.GetMethod("GetReal8", new[] { typeof(int) });
                                                                            if (getReal8Method != null)
                                                                                actualVal = getReal8Method.Invoke(seriesValuesObj, new object[] { totalActualCount });
                                                                        }
                                                                        else if (typeOfValueStr.Contains("Real4") || typeOfValueStr.Contains("Single") || typeOfValueStr.Contains("Float"))
                                                                        {
                                                                            var getReal4Method = vectorType.GetMethod("GetReal4", new[] { typeof(int) });
                                                                            if (getReal4Method != null)
                                                                                actualVal = getReal4Method.Invoke(seriesValuesObj, new object[] { totalActualCount });
                                                                        }
                                                                        else if (typeOfValueStr.Contains("Int4") || typeOfValueStr.Contains("Integer4"))
                                                                        {
                                                                            var getInt4Method = vectorType.GetMethod("GetInt4", new[] { typeof(int) });
                                                                            if (getInt4Method != null)
                                                                                actualVal = getInt4Method.Invoke(seriesValuesObj, new object[] { totalActualCount });
                                                                        }
                                                                        
                                                                        if (actualVal != null)
                                                                        {
                                                                            actualValues.Add(actualVal);
                                                                            totalActualCount++;
                                                                        }
                                                                        else
                                                                        {
                                                                            break; // No more values
                                                                        }
                                                                    }
                                                                    catch
                                                                    {
                                                                        break; // Index out of bounds
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        
                                                        if (actualValues.Count > valueCount && actualValues.Count > 50)
                                                        {
                                                            values = actualValues;
                                                            valueCount = actualValues.Count;
                                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                            {
                                                                DebugWriteLine(debug, $"      *** Used counts from GetValues() to access {actualValues.Count} actual values! ***");
                                                            }
                                                            result = actualValues; // Use actual values instead of counts
                                                        }
                                                    }
                                                }
                                            }
                                            // Try with parameters - might need count or start index
                                            else if (getValuesMethod.GetParameters().Length == 1)
                                            {
                                                var paramType = getValuesMethod.GetParameters()[0].ParameterType;
                                                if (paramType == typeof(int))
                                                {
                                                    // Try with different counts to see if we get more data
                                                    int[] testCounts = { 100, 500, 1000, 10000, int.MaxValue };
                                                    foreach (int testCount in testCounts)
                                                    {
                                                            try
                                                            {
                                                                var testResult = getValuesMethod.Invoke(seriesValuesObj, new object[] { testCount });
                                                                if (testResult != null && testResult is System.Collections.IEnumerable testEnum)
                                                                {
                                                                    int tempCount = testEnum.Cast<object?>().Count();
                                                                    if (tempCount > valueCount && tempCount >= 100)
                                                                    {
                                                                        values = testEnum.Cast<object?>();
                                                                        valueCount = tempCount;
                                                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                                        {
                                                                            DebugWriteLine(debug, $"    *** GetValues({testCount}) returned {tempCount} values! ***");
                                                                        }
                                                                        result = testResult;
                                                                        break;
                                                                    }
                                                                }
                                                            }
                                                            catch { }
                                                    }
                                                    if (result == null)
                                                    {
                                                        // Fallback to large number
                                                        result = getValuesMethod.Invoke(seriesValuesObj, new object[] { int.MaxValue });
                                                    }
                                                }
                                            }
                                            else if (getValuesMethod.GetParameters().Length == 2)
                                            {
                                                // Maybe start index and count
                                                result = getValuesMethod.Invoke(seriesValuesObj, new object[] { 0, 10000 });
                                            }
                                            
                                            if (result != null && result is System.Collections.IEnumerable resultEnum)
                                            {
                                                var tempValues = resultEnum.Cast<object?>();
                                                int tempCount = tempValues.Count();
                                                if (tempCount > valueCount)
                                                {
                                                    values = tempValues;
                                                    valueCount = tempCount;
                                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                    {
                                                        DebugWriteLine(debug, $"    GetValues({getValuesMethod.GetParameters().Length} params) returned {tempCount} values");
                                                    }
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                            {
                                            DebugWriteLine(debug, $"    Error calling GetValues({getValuesMethod.GetParameters().Length} params): {ex.Message}");
                                            }
                                        }
                                    }
                                }
                                
                                // If Size is much larger than what we got, try other methods
                                if (expectedSize.HasValue && expectedSize.Value > valueCount * 2)
                                {
                                    // Try other methods that might expand/decode the data
                                    foreach (var method in vectorType.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                                    {
                                        if ((method.Name.Contains("Get") || method.Name.Contains("To") || method.Name.Contains("Expand")) 
                                            && method.GetParameters().Length == 0)
                                        {
                                            try
                                            {
                                                var result = method.Invoke(seriesValuesObj, null);
                                                if (result != null && result is System.Collections.IEnumerable methodEnum)
                                                {
                                                    int methodCount = methodEnum.Cast<object?>().Count();
                                                    if (methodCount > valueCount)
                                                    {
                                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                        {
                                                            DebugWriteLine(debug, $"    {method.Name}() returned {methodCount} values");
                                                        }
                                                        values = methodEnum.Cast<object?>();
                                                        valueCount = methodCount;
                                                    }
                                                }
                                            }
                                            catch { }
                                        }
                                    }
                                }
                            }
                            
                            // Prioritize OriginalValues when it has exactly 96 values (correct full day data)
                            // OriginalValues is checked even if GetValues() returns more values
                            object? originalValuesObj = GetPropertyValue<object>(seriesInstance, "OriginalValues");
                            if (originalValuesObj != null && originalValuesObj is System.Collections.IEnumerable originalEnum)
                            {
                                int originalCount = 0;
                                List<object?> originalValueList = new List<object?>();
                                
                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                {
                                    DebugWriteLine(debug, $"    OriginalValues type: {originalValuesObj.GetType().Name}");
                                    DebugWriteLine(debug, $"    Checking OriginalValues contents...");
                                }
                                
                                foreach (var origItem in originalEnum)
                                {
                                    originalCount++;
                                    originalValueList.Add(origItem);
                                    
                                    // Debug first few items
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && originalCount <= 5)
                                    {
                                        DebugWriteLine(debug, $"      OriginalValues[{originalCount - 1}]: {origItem} (type: {origItem?.GetType().Name ?? "null"})");
                                        
                                        // Check if the item has properties that might contain collections
                                        if (origItem != null)
                                        {
                                            Type itemType = origItem.GetType();
                                            DebugWriteLine(debug, $"      Checking properties of OriginalValues[{originalCount - 1}] (type: {itemType.Name})...");
                                            
                                            var allProps = itemType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                                            DebugWriteLine(debug, $"        Found {allProps.Length} properties");
                                            
                                            foreach (var prop in allProps)
                                            {
                                                try
                                                {
                                                    var propVal = prop.GetValue(origItem);
                                                    if (propVal != null)
                                                    {
                                                        if (propVal is System.Collections.IEnumerable propEnum && !(propVal is string))
                                                        {
                                                            int propCount = 0;
                                                            List<object?> propValues = new List<object?>();
                                                            foreach (var pv in propEnum) 
                                                            { 
                                                                propCount++; 
                                                                propValues.Add(pv);
                                                                if (propCount > 10) break; 
                                                            }
                                                            DebugWriteLine(debug, $"        Property {prop.Name}: {propVal.GetType().Name} (count: {propCount})");
                                                            
                                                            if (propCount > valueCount && propCount > 50)
                                                            {
                                                                // Get full count
                                                                propCount = propEnum.Cast<object?>().Count();
                                                                values = propEnum.Cast<object?>();
                                                                valueCount = propCount;
                                                                DebugWriteLine(debug, $"        *** Found {propCount} values in property {prop.Name}! ***");
                                                                break;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            DebugWriteLine(debug, $"        Property {prop.Name}: {propVal} ({propVal.GetType().Name})");
                                                        }
                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    DebugWriteLine(debug, $"        Property {prop.Name}: Error - {ex.Message}");
                                                }
                                            }
                                            
                                            // Also check if the item itself might be a wrapper that contains a collection
                                            // Check all fields too, not just properties
                                            var allFields = itemType.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic);
                                            DebugWriteLine(debug, $"        Found {allFields.Length} fields");
                                            foreach (var field in allFields)
                                            {
                                                try
                                                {
                                                    var fieldVal = field.GetValue(origItem);
                                                    if (fieldVal != null && fieldVal is System.Collections.IEnumerable fieldEnum && !(fieldVal is string))
                                                    {
                                                        int fieldCount = fieldEnum.Cast<object?>().Count();
                                                        DebugWriteLine(debug, $"        Field {field.Name}: {fieldVal.GetType().Name} (count: {fieldCount})");
                                                        
                                                        if (fieldCount > valueCount && fieldCount > 50)
                                                        {
                                                            values = fieldEnum.Cast<object?>();
                                                            valueCount = fieldCount;
                                                            DebugWriteLine(debug, $"        *** Found {fieldCount} values in field {field.Name}! ***");
                                                            break;
                                                        }
                                                    }
                                                }
                                                catch { }
                                            }
                                        }
                                        
                                        // If the item is a collection or list, check what's inside
                                        if (origItem != null && origItem is System.Collections.IEnumerable nestedEnum && !(origItem is string))
                                        {
                                            int nestedCount = 0;
                                            List<object?> nestedList = new List<object?>();
                                            foreach (var nestedItem in nestedEnum)
                                            {
                                                nestedCount++;
                                                nestedList.Add(nestedItem);
                                                if (nestedCount <= 5)
                                                {
                                                    DebugWriteLine(debug, $"        Nested[{nestedCount - 1}]: {nestedItem} (type: {nestedItem?.GetType().Name ?? "null"})");
                                                }
                                            }
                                            DebugWriteLine(debug, $"        Total nested items: {nestedCount}");
                                            
                                            // If nested items found, use them as values
                                            if (nestedCount > valueCount && nestedCount > 50)
                                            {
                                                values = nestedList.Cast<object?>();
                                                valueCount = nestedCount;
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"        *** Using {nestedCount} nested values from OriginalValues! ***");
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                                
                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                {
                                    DebugWriteLine(debug, $"    Total OriginalValues count: {originalCount}");
                                }
                                
                                // Prioritize OriginalValues when it has exactly 96 values (full day at 15-minute intervals)
                                // This is the correct data source - ALWAYS use it even if GetValues() returned more values
                                if (originalCount == 96)
                                {
                                    // OriginalValues has exactly 96 values - this is the correct full day data
                                    // Override any previous values from GetValues() or other sources
                                    values = originalValueList.Cast<object?>();
                                    valueCount = originalCount;
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    *** PRIORITIZING {originalCount} values from OriginalValues (correct full day data)! ***");
                                    }
                                }
                                // Also check if OriginalValues has more data than we currently have (for other cases)
                                else if (originalCount > valueCount && originalCount > 50)
                                {
                                    // Check if OriginalValues contains nested collections
                                    bool foundNestedCollection = false;
                                    foreach (var origItem in originalValueList)
                                    {
                                        if (origItem != null && origItem is System.Collections.IEnumerable nestedEnum && !(origItem is string))
                                        {
                                            int nestedCount = nestedEnum.Cast<object?>().Count();
                                            if (nestedCount > valueCount && nestedCount > 50)
                                            {
                                                values = nestedEnum.Cast<object?>();
                                                valueCount = nestedCount;
                                                foundNestedCollection = true;
                                                if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                                {
                                                    DebugWriteLine(debug, $"    *** Found nested collection with {nestedCount} values in OriginalValues! ***");
                                                }
                                                break;
                                            }
                                        }
                                    }
                                    
                                    // If no nested collection found but OriginalValues itself has many items, use it
                                    if (!foundNestedCollection && originalCount > valueCount)
                                    {
                                        values = originalValueList.Cast<object?>();
                                        valueCount = originalCount;
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"    *** Using {originalCount} values from OriginalValues! ***");
                                        }
                                    }
                                }
                                else if (valueCount == 0 && originalCount > 0)
                                {
                                    // Fallback: use OriginalValues if we have nothing else
                                    values = originalValueList.Cast<object?>();
                                    valueCount = originalCount;
                                    if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                    {
                                        DebugWriteLine(debug, $"    *** Fallback: Using {originalCount} values from OriginalValues! ***");
                                    }
                                }
                            }
                            
                                // Check SeriesDefinition for storage method and how to expand data
                                if (seriesDef != null)
                                {
                                    var storageMethod = GetPropertyValue<object>(seriesDef, "StorageMethodID");
                                    var storageMethodStr = storageMethod?.ToString() ?? "unknown";
                                    
                                    // If storage method indicates compressed/incremental, we might need different expansion
                                    if (valueCount < 100)
                                    {
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                                        {
                                            DebugWriteLine(debug, $"    StorageMethodID: {storageMethodStr}");
                                            DebugWriteLine(debug, $"    Current value count: {valueCount}");
                                            
                                            // Check SeriesDefinition for number of points or samples
                                            Type defType = seriesDef.GetType();
                                            int? numberOfPoints = null;
                                            foreach (var prop in defType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                            {
                                                try
                                                {
                                                    var propVal = prop.GetValue(seriesDef);
                                                    if (propVal != null)
                                                    {
                                                        // Check if this is a count/number property
                                                        if (prop.Name.Contains("Count") || prop.Name.Contains("Number") || prop.Name.Contains("Point") || 
                                                            prop.Name.Contains("Sample") || prop.Name.Contains("Period") || prop.Name.Contains("Interval") ||
                                                            prop.Name.Contains("Nominal") || prop.Name.Contains("Quantity"))
                                                        {
                                                            DebugWriteLine(debug, $"    SeriesDefinition.{prop.Name}: {propVal} ({propVal.GetType().Name})");
                                                            
                                                            // If NumberOfPoints is much larger than valueCount, we need to expand
                                                            if ((prop.Name.Contains("Point") || prop.Name.Contains("Count")) && propVal is int countVal)
                                                            {
                                                                numberOfPoints = countVal;
                                                                if (countVal > valueCount * 10)
                                                                {
                                                                    DebugWriteLine(debug, $"    *** WARNING: SeriesDefinition indicates {countVal} points, but we only have {valueCount} values ***");
                                                                    DebugWriteLine(debug, $"    *** Need to expand data - maybe incremental/compressed storage ***");
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                catch { }
                                            }
                                            
                                            // Also check VectorElement for all properties - might contain the full data
                                            if (seriesValuesObj != null)
                                            {
                                                Type vectorType = seriesValuesObj.GetType();
                                                DebugWriteLine(debug, $"    Checking all VectorElement properties:");
                                                foreach (var prop in vectorType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                                {
                                                    try
                                                    {
                                                        var propVal = prop.GetValue(seriesValuesObj);
                                                        if (propVal != null)
                                                        {
                                                            DebugWriteLine(debug, $"      VectorElement.{prop.Name}: {propVal.GetType().Name}");
                                                            
                                                            // Check if it's an IEnumerable with many items
                                                            if (propVal is System.Collections.IEnumerable propEnum && !(propVal is string))
                                                            {
                                                                int propCount = 0;
                                                                foreach (var _ in propEnum) { propCount++; if (propCount > 100) break; }
                                                                if (propCount > valueCount && propCount > 50)
                                                                {
                                                                    DebugWriteLine(debug, $"        *** Found {prop.Name} with {propCount} values! ***");
                                                                    values = propEnum.Cast<object?>();
                                                                    valueCount = propCount;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    catch { }
                                                }
                                            }
                                        }
                                    }
                                }
                            
                            // Debug: If we're still getting few values, check if there's more data available
                            if (valueCount < 100 && observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                            {
                                DebugWriteLine(debug, $"    WARNING: Only got {valueCount} values, expected hundreds for full day data");
                                
                                // Check if SeriesInstance has methods to get all values
                                Type seriesInstanceType = seriesInstance.GetType();
                                DebugWriteLine(debug, $"    Checking SeriesInstance methods for data expansion:");
                                foreach (var method in seriesInstanceType.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                                {
                                    if (method.Name.Contains("Get") || method.Name.Contains("Expand") || method.Name.Contains("Original"))
                                    {
                                        try
                                        {
                                            if (method.GetParameters().Length == 0 && method.ReturnType != typeof(void))
                                            {
                                                var result = method.Invoke(seriesInstance, null);
                                                if (result != null && result is System.Collections.IEnumerable methodEnum)
                                                {
                                                    int methodCount = methodEnum.Cast<object?>().Count();
                                                    DebugWriteLine(debug, $"      {method.Name}() returned {methodCount} values");
                                                    if (methodCount > valueCount)
                                                    {
                                                        values = methodEnum.Cast<object?>();
                                                        valueCount = methodCount;
                                                        DebugWriteLine(debug, $"      *** Using {method.Name}() with {methodCount} values ***");
                                                    }
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            DebugWriteLine(debug, $"      {method.Name}() error: {ex.Message}");
                                        }
                                    }
                                }
                                
                                // Also check VectorElement for other methods
                                if (seriesValuesObj != null)
                                {
                                    Type vectorType = seriesValuesObj.GetType();
                                    DebugWriteLine(debug, $"    Checking VectorElement methods:");
                                    foreach (var method in vectorType.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                                    {
                                        if ((method.Name.Contains("Get") || method.Name.Contains("To") || method.Name.Contains("Expand")) 
                                            && method.GetParameters().Length <= 1)
                                        {
                                            try
                                            {
                                                object? result = null;
                                                if (method.GetParameters().Length == 0)
                                                {
                                                    result = method.Invoke(seriesValuesObj, null);
                                                }
                                                else if (method.GetParameters()[0].ParameterType == typeof(int))
                                                {
                                                    // Try with a large number
                                                    result = method.Invoke(seriesValuesObj, new object[] { 10000 });
                                                }
                                                
                                                if (result != null && result is System.Collections.IEnumerable methodEnum)
                                                {
                                                    int methodCount = methodEnum.Cast<object?>().Count();
                                                    
                                                    // Skip ToString() and other non-data methods
                                                    if (method.Name != "ToString" && method.Name != "GetHashCode" && method.Name != "Equals")
                                                    {
                                                        DebugWriteLine(debug, $"      VectorElement.{method.Name}() returned {methodCount} values");
                                                        
                                                        // Only use numeric/numeric-array results
                                                        bool isValidData = false;
                                                        try
                                                        {
                                                            var first = methodEnum.Cast<object?>().FirstOrDefault();
                                                            if (first != null)
                                                            {
                                                                var firstType = first.GetType();
                                                                isValidData = firstType.IsPrimitive || 
                                                                              firstType == typeof(decimal) || 
                                                                              firstType == typeof(float) || 
                                                                              firstType == typeof(double) ||
                                                                              firstType == typeof(int) ||
                                                                              firstType == typeof(long);
                                                            }
                                                        }
                                                        catch { }
                                                        
                                                        if (isValidData && methodCount > valueCount)
                                                        {
                                                            values = methodEnum.Cast<object?>();
                                                            valueCount = methodCount;
                                                            DebugWriteLine(debug, $"      *** Using VectorElement.{method.Name}() with {methodCount} values ***");
                                                        }
                                                    }
                                                }
                                            }
                                            catch { }
                                        }
                                    }
                                }
                            }
                            
                            // Debug: Print information about each series
                            if (observationIndex == 0 && channelIndex < 6) // Only print first few channels to reduce output
                            {
                                DebugWriteLine(debug, $"\n=== Channel {channelIndex}, Series {seriesIndex} ===");
                                DebugWriteLine(debug, $"  Column Name: {columnName}");
                                DebugWriteLine(debug, $"  values is null: {values == null}");
                                if (values != null)
                                {
                                    DebugWriteLine(debug, $"  Value count: {valueCount}");
                                }
                                
                                // Print first few values
                                if (values != null)
                                {
                                    int sampleCount = 0;
                                    DebugWriteLine(debug, $"  First 10 values:");
                                    foreach (var val in values)
                                    {
                                        if (sampleCount >= 10) break;
                                        DebugWriteLine(debug, $"    [{sampleCount}]: {val}");
                                        sampleCount++;
                                    }
                                }
                                
                                // Also check what other properties SeriesInstance has
                                Type siType = seriesInstance.GetType();
                                DebugWriteLine(debug, $"  SeriesInstance properties:");
                                foreach (var prop in siType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                                {
                                    if (prop.Name.Contains("Value") || prop.Name.Contains("Time") || prop.Name.Contains("Data") || prop.Name.Contains("Series"))
                                    {
                                        try
                                        {
                                            var propVal = prop.GetValue(seriesInstance);
                                            if (propVal != null)
                                            {
                                                if (propVal is System.Collections.IEnumerable enumVal && !(propVal is string))
                                                {
                                                    int count = 0;
                                                    foreach (var _ in enumVal) { count++; if (count > 3) break; }
                                                    DebugWriteLine(debug, $"    {prop.Name}: {propVal.GetType().Name} (count: {count})");
                                                }
                                                else
                                                {
                                                    DebugWriteLine(debug, $"    {prop.Name}: {propVal} ({propVal.GetType().Name})");
                                                }
                                            }
                                        }
                                        catch { }
                                    }
                                }
                            }

                            // Get time values - this might tell us how many time points we have
                            object? timeValuesObj = GetPropertyValue<object>(seriesInstance, "TimeValues");
                            List<DateTime?>? timeValues = null;
                            int timeValueCount = 0;
                            if (timeValuesObj != null && timeValuesObj is System.Collections.IEnumerable timeEnumerable)
                            {
                                timeValues = new List<DateTime?>();
                                foreach (var v in timeEnumerable)
                                {
                                    if (v is DateTime dt)
                                        timeValues.Add(dt);
                                    else if (v != null && v.GetType() == typeof(DateTime?))
                                        timeValues.Add((DateTime?)v);
                                    else
                                        timeValues.Add(null);
                                    timeValueCount++;
                                }
                            }
                            
                            // Debug: Check time value count vs data value count
                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2)
                            {
                                DebugWriteLine(debug, $"    Time values count: {timeValueCount}, Data values count: {valueCount}");
                                if (timeValueCount > valueCount && timeValueCount > 50)
                                {
                                    DebugWriteLine(debug, $"    *** Found {timeValueCount} time values - we need {timeValueCount} data values! ***");
                                    // Maybe we need to expand or interpolate data based on time values
                                }
                            }

                            if (values != null)
                            {
                                // Limit to 96 rows (one per 15-minute interval in a day = 24*4 = 96)
                                int maxRows = 96;
                                int rowIndex = 0;
                                foreach (var value in values)
                                {
                                    // Stop if we've reached the maximum number of rows
                                    if (rowIndex >= maxRows)
                                        break;
                                    
                                    // Check if this value is actually a count/size of a collection
                                    // If so, we need to access the actual collection, not the count
                                    object? actualValue = value;
                                    
                                    // The user said: "the value you retrieved for each column is not the value, is the number of values in this collection"
                                    // So each value might be a count that tells us how many items are in a collection
                                    // We need to check if the value is actually a collection count
                                    
                                    // Check if value is itself a collection/enumerable - if so, expand it
                                    if (value != null && value is System.Collections.IEnumerable valueCollection && !(value is string))
                                    {
                                        // Value IS a collection - iterate through each item in it
                                        int itemIndexInCollection = 0;
                                        foreach (var collectionItem in valueCollection)
                                        {
                                            // Each item in the collection becomes a separate row
                                            int actualRowIndex = rowIndex * 1000 + itemIndexInCollection; // Allow space for expansion
                                            
                                            // Ensure we have enough rows
                                            while (allDataRows.Count <= actualRowIndex)
                                            {
                                                allDataRows.Add(new Dictionary<string, object> { { "Time", null! } });
                                            }
                                            
                                            var itemRow = allDataRows[actualRowIndex];
                                            
                                            // Set timestamp for this item
                                            if (!itemRow.ContainsKey("Time") || itemRow["Time"] == null)
                                            {
                                                if (obsTime.HasValue)
                                                {
                                                    double? samplePeriod = seriesDef != null
                                                        ? GetPropertyValue<double?>(seriesDef, "SamplePeriod")
                                                        : null;
                                                    if (!samplePeriod.HasValue || samplePeriod.Value <= 0)
                                                        samplePeriod = 15.0;
                                                    itemRow["Time"] = obsTime.Value.AddSeconds(actualRowIndex * samplePeriod.Value);
                                                }
                                            }
                                            
                                            // Store the actual item value from the collection
                                            itemRow[columnName] = collectionItem ?? (object)"";
                                            itemIndexInCollection++;
                                            
                                            if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && rowIndex == 0 && itemIndexInCollection <= 5)
                                            {
                                                DebugWriteLine(debug, $"      Collection item[{itemIndexInCollection - 1}]: {collectionItem} (type: {collectionItem?.GetType().Name ?? "null"})");
                                            }
                                        }
                                        
                                        if (observationIndex == 0 && channelIndex < 2 && seriesIndex < 2 && rowIndex == 0 && itemIndexInCollection > 1)
                                        {
                                            DebugWriteLine(debug, $"    *** Value is a collection with {itemIndexInCollection} items - expanded! ***");
                                        }
                                        
                                        // Skip normal single-value processing since we already handled the collection
                                        rowIndex++;
                                        continue;
                                    }
                                    
                                    // Ensure we have enough rows
                                    while (allDataRows.Count <= rowIndex)
                                    {
                                        allDataRows.Add(new Dictionary<string, object> { { "Time", null! } });
                                    }

                                    var row = allDataRows[rowIndex];

                                    // Set timestamp if not already set
                                    if (!row.ContainsKey("Time") || row["Time"] == null)
                                    {
                                        if (timeValues != null)
                                        {
                                            var timeList = timeValues.ToList();
                                            if (rowIndex < timeList.Count)
                                            {
                                                var timeValue = timeList[rowIndex];
                                                if (timeValue.HasValue)
                                                    row["Time"] = timeValue.Value;
                                            }
                                        }
                                        else if (obsTime.HasValue)
                                        {
                                            // Try to get sample period - default to 15 seconds for trend data
                                            double? samplePeriod = null;
                                            if (seriesDef != null)
                                            {
                                                samplePeriod = GetPropertyValue<double?>(seriesDef, "SamplePeriod") ??
                                                              GetPropertyValue<double>(seriesDef, "SamplePeriod");
                                            }
                                            
                                            // Default to 15 seconds if not specified (common for trend data)
                                            if (!samplePeriod.HasValue || samplePeriod.Value <= 0)
                                            {
                                                samplePeriod = 15.0; // 15-second intervals
                                            }
                                            
                                            row["Time"] = obsTime.Value.AddSeconds(rowIndex * samplePeriod.Value);
                                        }
                                        else
                                        {
                                            // Format as MM:SS.0 (15-second intervals)
                                            int totalSeconds = rowIndex * 15;
                                            int minutes = totalSeconds / 60;
                                            int seconds = totalSeconds % 60;
                                            row["Time"] = $"{minutes:D2}:{seconds:D2}.0";
                                        }
                                    }
                                    
                                    // Format time as MM:SS.0 if it's a DateTime
                                    if (row["Time"] is DateTime dt)
                                    {
                                        int totalSeconds = (int)(dt - obsTime.GetValueOrDefault(dt)).TotalSeconds;
                                        int minutes = totalSeconds / 60;
                                        int seconds = totalSeconds % 60;
                                        row["Time"] = $"{minutes:D2}:{seconds:D2}.0";
                                    }

                                    // Set value - use the actual decoded physical value
                                    // Filter out timestamp values (integers that are seconds since midnight)
                                    object valueToWrite = value ?? (object)"";
                                    
                                    // Filter out timestamp-like values (e.g., 900=15min, 1800=30min, 28800=8hr, 29700=8.25hr)
                                    // These are seconds since midnight, not physical measurements
                                    // Timestamps are typically multiples of 15 (15-second intervals) or 60 (minute intervals)
                                    bool isTimestamp = false;
                                    if (valueToWrite is int intVal)
                                    {
                                        // Check if it looks like seconds since midnight (0-86400)
                                        // Exclude valid small integers (0-255 might be valid measurements)
                                        // But filter values that are multiples of 15 or 60 and in suspicious ranges
                                        if (intVal > 255 && intVal <= 86400)
                                        {
                                            // Check if it's a multiple of 15 or 60 (typical for timestamps)
                                            if (intVal % 15 == 0 || intVal % 60 == 0)
                                            {
                                                // This looks like a timestamp in seconds, skip it
                                                isTimestamp = true;
                                            }
                                        }
                                    }
                                    else if (valueToWrite is double d)
                                    {
                                        // Check if it's a suspiciously large integer-like value that might be a timestamp
                                        if (d > 255 && d <= 86400 && Math.Abs(d - Math.Round(d)) < 0.001)
                                        {
                                            int roundedIntVal = (int)Math.Round(d);
                                            // Check if it's a multiple of 15 or 60
                                            if (roundedIntVal % 15 == 0 || roundedIntVal % 60 == 0)
                                            {
                                                // Looks like a timestamp, skip it
                                                isTimestamp = true;
                                            }
                                        }
                                    }
                                    else if (valueToWrite is float f)
                                    {
                                        // Check if it's a suspiciously large integer-like value that might be a timestamp
                                        if (f > 255 && f <= 86400 && Math.Abs(f - Math.Round(f)) < 0.001)
                                        {
                                            int roundedFloatVal = (int)Math.Round(f);
                                            // Check if it's a multiple of 15 or 60
                                            if (roundedFloatVal % 15 == 0 || roundedFloatVal % 60 == 0)
                                            {
                                                // Looks like a timestamp, skip it
                                                isTimestamp = true;
                                            }
                                        }
                                    }
                                    
                                    // Skip timestamp values - they should not be written to data columns
                                    if (isTimestamp)
                                    {
                                        // Don't increment rowIndex, just skip this value and continue to next
                                        continue;
                                    }
                                    
                                    // Write the actual physical value
                                    // Don't filter out byte values - they might be valid measurements or decoded values
                                    // Just write whatever value we have (it should already be decoded from the byte array)
                                    row[columnName] = valueToWrite;
                                    
                                    // Check if this value is meaningful (not null, not zero, not empty)
                                    if (valueToWrite != null)
                                    {
                                        bool isMeaningful = false;
                                        if (valueToWrite is double d && Math.Abs(d) > 1e-10)
                                            isMeaningful = true;
                                        else if (valueToWrite is float f && Math.Abs(f) > 1e-10)
                                            isMeaningful = true;
                                        else if (valueToWrite is int i && i != 0)
                                            isMeaningful = true;
                                        else if (valueToWrite is long l && l != 0)
                                            isMeaningful = true;
                                        else if (valueToWrite is decimal m && m != 0)
                                            isMeaningful = true;
                                        else if (valueToWrite is string s && !string.IsNullOrWhiteSpace(s))
                                            isMeaningful = true;
                                        else if (!(valueToWrite is double || valueToWrite is float || valueToWrite is int || valueToWrite is long || valueToWrite is decimal || valueToWrite is string))
                                            isMeaningful = true; // Other types might be meaningful
                                        
                                        if (isMeaningful)
                                            columnsWithMeaningfulValues.Add(columnName);
                                    }

                                    rowIndex++;
                                }
                                
                                DebugWriteLine(debug, $"  Total rows processed: {rowIndex}");
                            }
                            else
                            {
                                DebugWriteLine(debug, $"  WARNING: values is null for {columnName}");
                            }

                            seriesIndex++;
                        }

                        channelIndex++;
                    }
                    
                    observationIndex++;
                }

                // Build CSV - ensure "Time" is first column
                // Filter out columns with non-meaningful values (only zeros, empty, etc.)
                List<string> columnList = allColumns.Where(col => 
                    col == "Time" || columnsWithMeaningfulValues.Contains(col)
                ).ToList();
                columnList.Remove("Time");
                columnList.Insert(0, "Time");
                StringBuilder csvContent = new StringBuilder();
                
                // Write header
                csvContent.AppendLine(string.Join(",", EscapeCsvValues(columnList)));

                // Write data rows
                foreach (var row in allDataRows)
                {
                    List<string> csvRow = new List<string>();
                    foreach (var column in columnList)
                    {
                        object value = row.ContainsKey(column) ? row[column] : "";
                        
                        if (value is DateTime dt)
                            csvRow.Add(dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
                        else
                            csvRow.Add(value?.ToString() ?? "");
                    }
                    csvContent.AppendLine(string.Join(",", EscapeCsvValues(csvRow)));
                }

                // Write to file
                await File.WriteAllTextAsync(outputFile, csvContent.ToString(), Encoding.UTF8);
                
                Console.WriteLine($"Successfully converted {inputFile} to {outputFile}");
                // Intentionally quiet: avoid noisy summary lines.
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred while processing the PQDIF file:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        static T? GetPropertyValue<T>(object? obj, string propertyName)
        {
            if (obj == null)
                return default;
            
            Type type = obj.GetType();
            PropertyInfo? prop = type.GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
            
            if (prop != null && prop.CanRead)
            {
                try
                {
                    object? value = prop.GetValue(obj);
                    if (value is T directValue)
                        return directValue;
                    
                    // Try conversion
                    if (value != null)
                    {
                        Type targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
                        if (targetType.IsEnum && value is string enumText)
                            return (T)Enum.Parse(targetType, enumText, true);
                        if (value.GetType() == targetType)
                            return (T)Convert.ChangeType(value, targetType);
                    }
                }
                catch { }
            }
            
            return default;
        }

        static string MapQuantityType(string qtyStr)
        {
            if (string.IsNullOrEmpty(qtyStr))
                return "";
            
            // Map to common quantity types based on your desired format
            // Voltage/Current -> RMS, Power types -> S Tot/Q Tot/P Tot
            if (qtyStr.Equals("RMS", StringComparison.OrdinalIgnoreCase) || 
                qtyStr.Equals("Rms", StringComparison.OrdinalIgnoreCase))
                return "RMS";
            else if (qtyStr.Equals("Voltage", StringComparison.OrdinalIgnoreCase) || 
                     qtyStr.Contains("Voltage", StringComparison.OrdinalIgnoreCase))
                return "RMS";
            else if (qtyStr.Equals("Current", StringComparison.OrdinalIgnoreCase) || 
                     qtyStr.Contains("Current", StringComparison.OrdinalIgnoreCase))
                return "RMS";
            else if (qtyStr.Contains("ApparentPower", StringComparison.OrdinalIgnoreCase) || 
                     qtyStr.Contains("Apparent", StringComparison.OrdinalIgnoreCase))
                return "S Tot";
            else if (qtyStr.Contains("ReactivePower", StringComparison.OrdinalIgnoreCase) || 
                     qtyStr.Contains("Reactive", StringComparison.OrdinalIgnoreCase))
                return "Q Tot";
            else if (qtyStr.Contains("ActivePower", StringComparison.OrdinalIgnoreCase) || 
                     qtyStr.Contains("Active", StringComparison.OrdinalIgnoreCase) || 
                     qtyStr.Contains("Real", StringComparison.OrdinalIgnoreCase))
                return "P Tot";
            else if (qtyStr.Equals("Power", StringComparison.OrdinalIgnoreCase) ||
                     qtyStr.Contains("Power", StringComparison.OrdinalIgnoreCase))
                return "P Tot";
            else if (qtyStr.Length < 50)
                return qtyStr;
            
            return "";
        }

        static void DebugWriteLine(bool debug, string message)
        {
            if (debug)
                Console.WriteLine(message);
        }
        
        static List<string> EscapeCsvValues(IEnumerable<string> values)
        {
            List<string> escaped = new List<string>();
            foreach (var value in values)
            {
                if (value != null && (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r")))
                {
                    escaped.Add($"\"{value.Replace("\"", "\"\"")}\"");
                }
                else
                {
                    escaped.Add(value ?? "");
                }
            }
            return escaped;
        }
    }
}
