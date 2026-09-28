namespace T28.V2.Receiver;

/// <summary>Committed, non-operational identity and receive-frequency authority for v2 profiles.</summary>
public sealed record ApprovedChannelDefinition(string PlanId, string PlanName, string PlanGroup, string Number, string DisplayName, long FrequencyHz);

public static class ApprovedChannelCatalogue
{
    private static readonly ApprovedChannelDefinition[] Entries = Build();
    private static readonly Dictionary<(string PlanId, string Number), ApprovedChannelDefinition> ByPlanNumber = Entries.ToDictionary(value => (value.PlanId, value.Number));

    public static IReadOnlyList<ApprovedChannelDefinition> All => Entries;
    public static bool TryGet(string planId, string number, out ApprovedChannelDefinition? value) => ByPlanNumber.TryGetValue((planId, number), out value);

    private static ApprovedChannelDefinition[] Build()
    {
        List<ApprovedChannelDefinition> values = [];
        AddCell(values, "metro-north-coastal", "Demo location 47", [("Demo location 27","247","Demo location 13",145750000,"354-359"),("Demo location 28","294","Demo location 53",145337500,"354-359"),("Demo location 29","133","Demo location 8",145500000,"360-365"),("Demo location 30","153","Demo location 55",145537500,"354-359"),("Demo location 31","224","Demo location 53",145312500,"360-365"),("Demo location 32","261","Demo location 60",145300000,"354-359")]);
        AddCell(values, "metro-north-east", "Demo location 48", [("Demo location 33","346","Demo location 65",145850000,"360-365"),("Demo location 34","110","Demo location 3",145787500,"354-359"),("Demo location 35","112","Demo location 51",145462500,"360-365")]);
        AddCell(values, "metro-central-coastal", "Demo location 45", [("Demo location 14","231","Demo location 64",145625000,"354-359"),("Demo location 15","173","Demo location 64",145400000,"360-365"),("Demo location 16","316","Demo location 57",145475000,"354-359"),("Demo location 17","229","Demo location 9",145562500,"360-365"),("Demo location 18","138","Demo location 44",145725000,"354-359"),("Demo location 19","121","Demo location 5",145487500,"360-365")]);
        AddCell(values, "metro-central-east", "Demo location 46", [("Demo location 20","142","Demo Hill",145812500,"360-365"),("Demo location 21","260","Demo location 51",145600000,"354-359"),("Demo location 22","206","Demo location 9",145775000,"360-365"),("Demo location 23","238","Demo location 56",145362500,"354-359"),("Demo location 24","141","Demo Hill",145575000,"354-359"),("Demo location 25","144","Demo location 56",145637500,"354-359"),("Demo location 26","128","Demo location 11",145450000,"360-365")]);
        AddCell(values, "metro-south-coastal", "Demo location 49", [("Demo location 36","109","Demo location 2",145675000,"354-359"),("Demo location 37","234","Demo location 2",145387500,"360-365"),("Demo location 38","168","Demo location 6",145325000,"354-359"),("Demo location 39","161","Demo Hill",145350000,"354-359")]);
        AddCell(values, "metro-south-east", "Demo location 50", [("Demo location 40","221","Demo location 12",145862500,"360-365"),("Demo location 41","218","Demo location 59",145825000,"354-359"),("Demo location 42","139","Demo location 6",145662500,"360-365"),("Demo location 43","170","Demo location 12",145550000,"354-359")]);
        // 2024 Metro All Hazards guide p.9 is the membership and display-label
        // authority. WARSUG's published paired 6IP/6AR guide supplies the
        // mid-band receive frequencies omitted by the older WAERN workbook.
        AddNetworkControl(values);
        Add(values, "shared-air-operations", "Demo location 58", "Other approved categories", "644", "Demo location 62", 145712500);
        Add(values, "shared-air-operations", "Demo location 58", "Other approved categories", "621", "Demo location 63", 145437500);
        Add(values, "shared-air-operations", "Demo location 58", "Other approved categories", "369", "Demo location 1", 145250000);
        return values.ToArray();
    }

    private static void AddNetworkControl(List<ApprovedChannelDefinition> values)
    {
        (string Number, string Name, long Frequency)[] channels = [("6","Demo location 13",145037500),("24","Demo location 4",145075000),("25","Demo location 11",145087500),("30","Demo location 3",145112500),("52","Demo location 8",145000000),("53","Demo location 61",145012500),("55","Demo Hill",145025000),("56","Demo location 54",145037500),("57","Demo location 55",145050000),("58","Demo location 65",145062500),("77","Demo location 60",145100000),("80","Demo location 12",145112500),("92","Demo location 10",145125000),("105","Demo location 3",145425000),("205","Demo location 9",145412500),("207","Demo location 11",145650000),("217","Demo location 54",145687500),("220","Demo location 12",145700000),("246","Demo location 13",145612500),("263","Demo location 60",145375000),("317","Demo location 10",145800000),("328","Demo location 4",145587500),("371","Demo Hill",145512500),("373","Demo location 8",145525000),("376","Demo location 65",145737500),("377","Demo location 61",145762500),("379","Demo location 5",145837500),("380","Demo location 55",145875000)];
        foreach ((string number, string name, long frequency) in channels) Add(values, "network-control", "Demo location 52", "Other approved categories", number, name, frequency);
    }

    private static void AddCell(List<ApprovedChannelDefinition> values, string cellId, string cellName, (string Plan, string Number, string Name, long Frequency, string Sectors)[] plans)
    {
        long[] sectorFrequencies = [145225000,145237500,145275000,145262500,145150000,145137500,145287500,145162500,145175000,145200000,145187500,145212500];
        foreach ((string plan, string number, string name, long frequency, string sectors) in plans)
        {
            string planId = plan.ToLowerInvariant().Replace(' ', '-'); Add(values, planId, plan, cellName, number, name, frequency);
            int first = sectors == "354-359" ? 0 : 6;
            for (int index = 0; index < 6; index++) Add(values, planId, plan, cellName, (354 + first + index).ToString(), "Sector " + (354 + first + index), sectorFrequencies[first + index]);
        }
    }

    private static void Add(List<ApprovedChannelDefinition> values, string planId, string planName, string planGroup, string number, string name, long frequency) => values.Add(new(planId, planName, planGroup, number, name, frequency));
}
