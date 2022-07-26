namespace Common
{
    public static class NewtonsoftJsonSafeInit
    {
        private static bool isInitialized = false;

        public static void SetDefaultSettings()
        {
            if (!isInitialized)
            {
                // Fixes https://github.com/advisories/GHSA-5crp-9r3c-p9vr
                // Improper Handling of Exceptional Conditions in Newtonsoft.Json
                Newtonsoft.Json.JsonConvert.DefaultSettings = () => new Newtonsoft.Json.JsonSerializerSettings { MaxDepth = 128 };
                isInitialized = true;
            }
        }
    }
}
