﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;

namespace TesApi.Web
{
    /// <summary>
    /// Utility class that provides mapping between VM names and billing meters
    /// </summary>
    public static class AzureBillingUtils
    {
        /// <summary>
        /// Lists all VM sizes verified supported by Azure Batch, along with the family and billing meter names
        /// </summary>
        /// <returns>List of VM sizes</returns>
        public static List<(string VmSize, string MeterName, string MeterSubCategory)> GetVmSizesSupportedByBatch()
            => VmSizesFamiliesAndMeters.Where(v => VerifiedVmSizes.Contains(v.VmSize, StringComparer.OrdinalIgnoreCase)).ToList();

        private static IEnumerable<(string VmSize, string MeterName, string MeterSubCategory)> VmSizesFamiliesAndMeters => new[]
        {
            ("Standard_A2_v2", "A2 v2", "Av2 Series"),
            ("Standard_A2m_v2", "A2m v2", "Av2 Series"),
            ("Standard_A4_v2", "A4 v2", "Av2 Series"),
            ("Standard_A4m_v2", "A4m v2", "Av2 Series"),
            ("Standard_A8_v2", "A8 v2", "Av2 Series"),
            ("Standard_A8m_v2", "A8m v2", "Av2 Series"),
            ("Standard_D1_v2", "D1 v2/DS1 v2", "Dv2/DSv2 Series"),
            ("Standard_D11_v2", "D11 v2/DS11 v2", "Dv2/DSv2 Series"),
            ("Standard_D12_v2", "D12 v2/DS12 v2", "Dv2/DSv2 Series"),
            ("Standard_D13_v2", "D13 v2/DS13 v2", "Dv2/DSv2 Series"),
            ("Standard_D14_v2", "D14 v2/DS14 v2", "Dv2/DSv2 Series"),
            ("Standard_D15_v2", "D15 v2/DS15 v2", "Dv2/DSv2 Series"),
            ("Standard_D16_v3", "D16 v3/D16s v3", "Dv3/DSv3 Series"),
            ("Standard_D16a_v4", "D16a v4/D16as v4", "Dav4/Dasv4 Series"),
            ("Standard_D16ads_v5", "D16ads v5", "Dadsv5 Series"),
            ("Standard_D16as_v4", "D16a v4/D16as v4", "Dav4/Dasv4 Series"),
            ("Standard_D16d_v4", "D16d v4", "Ddv4 Series"),
            ("Standard_D16d_v5", "D16d v5", "Ddv5 Series"),
            ("Standard_D16ds_v4", "D16ds v4", "Ddsv4 Series"),
            ("Standard_D16ds_v5", "D16ds v5", "Ddsv5 Series"),
            ("Standard_D16s_v3", "D16 v3/D16s v3", "Dv3/DSv3 Series"),
            ("Standard_D2_v2", "D2 v2/DS2 v2", "Dv2/DSv2 Series"),
            ("Standard_D2_v3", "D2 v3/D2s v3", "Dv3/DSv3 Series"),
            ("Standard_D2a_v4", "D2a v4/D2as v4", "Dav4/Dasv4 Series"),
            ("Standard_D2ads_v5", "D2ads v5", "Dadsv5 Series"),
            ("Standard_D2as_v4", "D2a v4/D2as v4", "Dav4/Dasv4 Series"),
            ("Standard_D2d_v4", "D2d v4", "Ddv4 Series"),
            ("Standard_D2d_v5", "D2d v5", "Ddv5 Series"),
            ("Standard_D2ds_v4", "D2ds v4", "Ddsv4 Series"),
            ("Standard_D2ds_v5", "D2ds v5", "Ddsv5 Series"),
            ("Standard_D2s_v3", "D2 v3/D2s v3", "Dv3/DSv3 Series"),
            ("Standard_D3_v2", "D3 v2/DS3 v2", "Dv2/DSv2 Series"),
            ("Standard_D32_v3", "D32 v3/D32s v3", "Dv3/DSv3 Series"),
            ("Standard_D32a_v4", "D32a v4/D32as v4", "Dav4/Dasv4 Series"),
            ("Standard_D32ads_v5", "D32ads v5", "Dadsv5 Series"),
            ("Standard_D32as_v4", "D32a v4/D32as v4", "Dav4/Dasv4 Series"),
            ("Standard_D32d_v4", "D32d v4", "Ddv4 Series"),
            ("Standard_D32d_v5", "D32d v5", "Ddv5 Series"),
            ("Standard_D32ds_v4", "D32ds v4", "Ddsv4 Series"),
            ("Standard_D32ds_v5", "D32ds v5", "Ddsv5 Series"),
            ("Standard_D32s_v3", "D32 v3/D32s v3", "Dv3/DSv3 Series"),
            ("Standard_D4_v2", "D4 v2/DS4 v2", "Dv2/DSv2 Series"),
            ("Standard_D4_v3", "D4 v3/D4s v3", "Dv3/DSv3 Series"),
            ("Standard_D48_v3", "D48 v3/D48s v3", "Dv3/DSv3 Series"),
            ("Standard_D48a_v4", "D48a v4/D48as v4", "Dav4/Dasv4 Series"),
            ("Standard_D48ads_v5", "D48ads v5", "Dadsv5 Series"),
            ("Standard_D48as_v4", "D48a v4/D48as v4", "Dav4/Dasv4 Series"),
            ("Standard_D48d_v4", "D48d v4", "Ddv4 Series"),
            ("Standard_D48d_v5", "D48d v5", "Ddv5 Series"),
            ("Standard_D48ds_v4", "D48ds v4", "Ddsv4 Series"),
            ("Standard_D48ds_v5", "D48ds v5", "Ddsv5 Series"),
            ("Standard_D48s_v3", "D48 v3/D48s v3", "Dv3/DSv3 Series"),
            ("Standard_D4a_v4", "D4a v4/D4as v4", "Dav4/Dasv4 Series"),
            ("Standard_D4ads_v5", "D4ads v5", "Dadsv5 Series"),
            ("Standard_D4as_v4", "D4a v4/D4as v4", "Dav4/Dasv4 Series"),
            ("Standard_D4d_v4", "D4d v4", "Ddv4 Series"),
            ("Standard_D4d_v5", "D4d v5", "Ddv5 Series"),
            ("Standard_D4ds_v4", "D4ds v4", "Ddsv4 Series"),
            ("Standard_D4ds_v5", "D4ds v5", "Ddsv5 Series"),
            ("Standard_D4s_v3", "D4 v3/D4s v3", "Dv3/DSv3 Series"),
            ("Standard_D5_v2", "D5 v2/DS5 v2", "Dv2/DSv2 Series"),
            ("Standard_D64_v3", "D64 v3/D64s v3", "Dv3/DSv3 Series"),
            ("Standard_D64a_v4", "D64a v4/D64as v4", "Dav4/Dasv4 Series"),
            ("Standard_D64ads_v5", "D64ads v5", "Dadsv5 Series"),
            ("Standard_D64as_v4", "D64a v4/D64as v4", "Dav4/Dasv4 Series"),
            ("Standard_D64d_v4", "D64d v4", "Ddv4 Series"),
            ("Standard_D64d_v5", "D64d v5", "Ddv5 Series"),
            ("Standard_D64ds_v4", "D64ds v4", "Ddsv4 Series"),
            ("Standard_D64ds_v5", "D64ds v5", "Ddsv5 Series"),
            ("Standard_D64s_v3", "D64 v3/D64s v3", "Dv3/DSv3 Series"),
            ("Standard_D8_v3", "D8 v3/D8s v3", "Dv3/DSv3 Series"),
            ("Standard_D8a_v4", "D8a v4/D8as v4", "Dav4/Dasv4 Series"),
            ("Standard_D8ads_v5", "D8ads v5", "Dadsv5 Series"),
            ("Standard_D8as_v4", "D8a v4/D8as v4", "Dav4/Dasv4 Series"),
            ("Standard_D8d_v4", "D8d v4", "Ddv4 Series"),
            ("Standard_D8d_v5", "D8d v5", "Ddv5 Series"),
            ("Standard_D8ds_v4", "D8ds v4", "Ddsv4 Series"),
            ("Standard_D8ds_v5", "D8ds v5", "Ddsv5 Series"),
            ("Standard_D8s_v3", "D8 v3/D8s v3", "Dv3/DSv3 Series"),
            ("Standard_D96a_v4", "D96a v4/D96as v4", "Dav4/Dasv4 Series"),
            ("Standard_D96ads_v5", "D96ads v5", "Dadsv5 Series"),
            ("Standard_D96as_v4", "D96a v4/D96as v4", "Dav4/Dasv4 Series"),
            ("Standard_D96d_v5", "D96d v5", "Ddv5 Series"),
            ("Standard_D96ds_v5", "D96ds v5", "Ddsv5 Series"),
            ("Standard_DS1_v2", "D1 v2/DS1 v2", "Dv2/DSv2 Series"),
            ("Standard_DS11_v2", "D11 v2/DS11 v2", "Dv2/DSv2 Series"),
            ("Standard_DS11-1_v2", "D11 v2/DS11 v2", "Dv2/DSv2 Series"),
            ("Standard_DS12_v2", "D12 v2/DS12 v2", "Dv2/DSv2 Series"),
            ("Standard_DS12-1_v2", "D12 v2/DS12 v2", "Dv2/DSv2 Series"),
            ("Standard_DS12-2_v2", "D12 v2/DS12 v2", "Dv2/DSv2 Series"),
            ("Standard_DS13_v2", "D13 v2/DS13 v2", "Dv2/DSv2 Series"),
            ("Standard_DS13-2_v2", "D13 v2/DS13 v2", "Dv2/DSv2 Series"),
            ("Standard_DS13-4_v2", "D13 v2/DS13 v2", "Dv2/DSv2 Series"),
            ("Standard_DS14_v2", "D14 v2/DS14 v2", "Dv2/DSv2 Series"),
            ("Standard_DS14-4_v2", "D14 v2/DS14 v2", "Dv2/DSv2 Series"),
            ("Standard_DS14-8_v2", "D14 v2/DS14 v2", "Dv2/DSv2 Series"),
            ("Standard_DS15_v2", "D15 v2/DS15 v2", "Dv2/DSv2 Series"),
            ("Standard_DS2_v2", "D2 v2/DS2 v2", "Dv2/DSv2 Series"),
            ("Standard_DS3_v2", "D3 v2/DS3 v2", "Dv2/DSv2 Series"),
            ("Standard_DS4_v2", "D4 v2/DS4 v2", "Dv2/DSv2 Series"),
            ("Standard_DS5_v2", "D5 v2/DS5 v2", "Dv2/DSv2 Series"),
            ("Standard_E16_v3", "E16 v3/E16s v3", "Ev3/ESv3 Series"),
            ("Standard_E16-4ads_v5", "E16-4ads v5", "Eadsv5 Series"),
            ("Standard_E16-4as_v4", "E16-4as_v4", "Eav4/Easv4 Series"),
            ("Standard_E16-4ds_v4", "E16-4ds v4", "Edsv4 Series"),
            ("Standard_E16-4ds_v5", "E16ds v5", "Edsv5 Series"),
            ("Standard_E16-4s_v3", "E16 v3/E16s v3", "Ev3/ESv3 Series"),
            ("Standard_E16-8ads_v5", "E16-8ads v5", "Eadsv5 Series"),
            ("Standard_E16-8as_v4", "E16-8as_v4", "Eav4/Easv4 Series"),
            ("Standard_E16-8ds_v4", "E16-8ds v4", "Edsv4 Series"),
            ("Standard_E16-8ds_v5", "E16ds v5", "Edsv5 Series"),
            ("Standard_E16-8s_v3", "E16 v3/E16s v3", "Ev3/ESv3 Series"),
            ("Standard_E16a_v4", "E16a v4/E16as v4", "Eav4/Easv4 Series"),
            ("Standard_E16ads_v5", "E16ads v5", "Eadsv5 Series"),
            ("Standard_E16as_v4", "E16a v4/E16as v4", "Eav4/Easv4 Series"),
            ("Standard_E16d_v4", "E16d v4", "Edv4 Series"),
            ("Standard_E16d_v5", "E16d v5", "Edv5 Series"),
            ("Standard_E16ds_v4", "E16ds v4", "Edsv4 Series"),
            ("Standard_E16ds_v5", "E16ds v5", "Edsv5 Series"),
            ("Standard_E16s_v3", "E16 v3/E16s v3", "Ev3/ESv3 Series"),
            ("Standard_E2_v3", "E2 v3/E2s v3", "Ev3/ESv3 Series"),
            ("Standard_E20_v3", "E20 v3/E20s v3", "Ev3/ESv3 Series"),
            ("Standard_E20a_v4", "E20a v4/E20as v4", "Eav4/Easv4 Series"),
            ("Standard_E20ads_v5", "E20ads v5", "Eadsv5 Series"),
            ("Standard_E20as_v4", "E20a v4/E20as v4", "Eav4/Easv4 Series"),
            ("Standard_E20d_v4", "E20d v4", "Edv4 Series"),
            ("Standard_E20d_v5", "E20d v5", "Edv5 Series"),
            ("Standard_E20ds_v4", "E20ds v4", "Edsv4 Series"),
            ("Standard_E20ds_v5", "E20ds v5", "Edsv5 Series"),
            ("Standard_E20s_v3", "E20 v3/E20s v3", "Ev3/ESv3 Series"),
            ("Standard_E2a_v4", "E2a v4/E2as v4", "Eav4/Easv4 Series"),
            ("Standard_E2ads_v5", "E2ads v5", "Eadsv5 Series"),
            ("Standard_E2as_v4", "E2a v4/E2as v4", "Eav4/Easv4 Series"),
            ("Standard_E2d_v4", "E2d v4", "Edv4 Series"),
            ("Standard_E2d_v5", "E2d v5", "Edv5 Series"),
            ("Standard_E2ds_v4", "E2ds v4", "Edsv4 Series"),
            ("Standard_E2ds_v5", "E2ds v5", "Edsv5 Series"),
            ("Standard_E2s_v3", "E2 v3/E2s v3", "Ev3/ESv3 Series"),
            ("Standard_E32_v3", "E32 v3/E32s v3", "Ev3/ESv3 Series"),
            ("Standard_E32-16ads_v5", "E32-16ads v5", "Eadsv5 Series"),
            ("Standard_E32-16as_v4", "E32-16as_v4", "Eav4/Easv4 Series"),
            ("Standard_E32-16ds_v4", "E32-16ds v4", "Edsv4 Series"),
            ("Standard_E32-16ds_v5", "E32ds v5", "Edsv5 Series"),
            ("Standard_E32-16s_v3", "E32 v3/E32s v3", "Ev3/ESv3 Series"),
            ("Standard_E32-8ads_v5", "E32-8ads v5", "Eadsv5 Series"),
            ("Standard_E32-8as_v4", "E32-8as_v4", "Eav4/Easv4 Series"),
            ("Standard_E32-8ds_v4", "E32-8ds v4", "Edsv4 Series"),
            ("Standard_E32-8ds_v5", "E32ds v5", "Edsv5 Series"),
            ("Standard_E32-8s_v3", "E32 v3/E32s v3", "Ev3/ESv3 Series"),
            ("Standard_E32a_v4", "E32a v4/E32as v4", "Eav4/Easv4 Series"),
            ("Standard_E32ads_v5", "E32ads v5", "Eadsv5 Series"),
            ("Standard_E32as_v4", "E32a v4/E32as v4", "Eav4/Easv4 Series"),
            ("Standard_E32d_v4", "E32d v4", "Edv4 Series"),
            ("Standard_E32d_v5", "E32d v5", "Edv5 Series"),
            ("Standard_E32ds_v4", "E32ds v4", "Edsv4 Series"),
            ("Standard_E32ds_v5", "E32ds v5", "Edsv5 Series"),
            ("Standard_E32s_v3", "E32 v3/E32s v3", "Ev3/ESv3 Series"),
            ("Standard_E4_v3", "E4 v3/E4s v3", "Ev3/ESv3 Series"),
            ("Standard_E4-2ads_v5", "E4-2ads v5", "Eadsv5 Series"),
            ("Standard_E4-2as_v4", "E4-2as_v4", "Eav4/Easv4 Series"),
            ("Standard_E4-2ds_v4", "E4-2ds v4", "Edsv4 Series"),
            ("Standard_E4-2ds_v5", "E4ds v5", "Edsv5 Series"),
            ("Standard_E4-2s_v3", "E4 v3/E4s v3", "Ev3/ESv3 Series"),
            ("Standard_E48_v3", "E48 v3/E48s v3", "Ev3/ESv3 Series"),
            ("Standard_E48a_v4", "E48a v4/E48as v4", "Eav4/Easv4 Series"),
            ("Standard_E48ads_v5", "E48ads v5", "Eadsv5 Series"),
            ("Standard_E48as_v4", "E48a v4/E48as v4", "Eav4/Easv4 Series"),
            ("Standard_E48d_v4", "E48d v4", "Edv4 Series"),
            ("Standard_E48d_v5", "E48d v5", "Edv5 Series"),
            ("Standard_E48ds_v4", "E48ds v4", "Edsv4 Series"),
            ("Standard_E48ds_v5", "E48ds v5", "Edsv5 Series"),
            ("Standard_E48s_v3", "E48 v3/E48s v3", "Ev3/ESv3 Series"),
            ("Standard_E4a_v4", "E4a v4/E4as v4", "Eav4/Easv4 Series"),
            ("Standard_E4ads_v5", "E4ads v5", "Eadsv5 Series"),
            ("Standard_E4as_v4", "E4a v4/E4as v4", "Eav4/Easv4 Series"),
            ("Standard_E4d_v4", "E4d v4", "Edv4 Series"),
            ("Standard_E4d_v5", "E4d v5", "Edv5 Series"),
            ("Standard_E4ds_v4", "E4ds v4", "Edsv4 Series"),
            ("Standard_E4ds_v5", "E4ds v5", "Edsv5 Series"),
            ("Standard_E4s_v3", "E4 v3/E4s v3", "Ev3/ESv3 Series"),
            ("Standard_E64_v3", "E64 v3/E64s v3", "Ev3/ESv3 Series"),
            ("Standard_E64-16ads_v5", "E64-16ads v5", "Eadsv5 Series"),
            ("Standard_E64-16as_v4", "E64-16as_v4", "Eav4/Easv4 Series"),
            ("Standard_E64-16ds_v4", "E64-16ds v4", "Edsv4 Series"),
            ("Standard_E64-16ds_v5", "E64ds v5", "Edsv5 Series"),
            ("Standard_E64-16s_v3", "E64 v3/E64s v3", "Ev3/ESv3 Series"),
            ("Standard_E64-32ads_v5", "E64-32ads v5", "Eadsv5 Series"),
            ("Standard_E64-32as_v4", "E64-32as_v4", "Eav4/Easv4 Series"),
            ("Standard_E64-32ds_v4", "E64-32ds v4", "Edsv4 Series"),
            ("Standard_E64-32ds_v5", "E64ds v5", "Edsv5 Series"),
            ("Standard_E64-32s_v3", "E64 v3/E64s v3", "Ev3/ESv3 Series"),
            ("Standard_E64a_v4", "E64a v4/E64as v4", "Eav4/Easv4 Series"),
            ("Standard_E64ads_v5", "E64ads v5", "Eadsv5 Series"),
            ("Standard_E64as_v4", "E64a v4/E64as v4", "Eav4/Easv4 Series"),
            ("Standard_E64d_v4", "E64d v4", "Edv4 Series"),
            ("Standard_E64d_v5", "E64d v5", "Edv5 Series"),
            ("Standard_E64ds_v4", "E64ds v4", "Edsv4 Series"),
            ("Standard_E64ds_v5", "E64ds v5", "Edsv5 Series"),
            ("Standard_E64i_v3", "E64i v3/E64is v3", "Ev3/ESv3 Series"),
            ("Standard_E64s_v3", "E64 v3/E64s v3", "Ev3/ESv3 Series"),
            ("Standard_E8_v3", "E8 v3/E8s v3", "Ev3/ESv3 Series"),
            ("Standard_E8-2ads_v5", "E8-2ads v5", "Eadsv5 Series"),
            ("Standard_E8-2as_v4", "E8-2as_v4", "Eav4/Easv4 Series"),
            ("Standard_E8-2ds_v4", "E8-2ds v4", "Edsv4 Series"),
            ("Standard_E8-2ds_v5", "E8ds v5", "Edsv5 Series"),
            ("Standard_E8-2s_v3", "E8 v3/E8s v3", "Ev3/ESv3 Series"),
            ("Standard_E8-4ads_v5", "E8-4ads v5", "Eadsv5 Series"),
            ("Standard_E8-4as_v4", "E8-4as_v4", "Eav4/Easv4 Series"),
            ("Standard_E8-4ds_v4", "E8-4ds v4", "Edsv4 Series"),
            ("Standard_E8-4ds_v5", "E8ds v5", "Edsv5 Series"),
            ("Standard_E8-4s_v3", "E8 v3/E8s v3", "Ev3/ESv3 Series"),
            ("Standard_E80ids_v4", "E80ids v4", "Edsv4 Series"),
            ("Standard_E8a_v4", "E8a v4/E8as v4", "Eav4/Easv4 Series"),
            ("Standard_E8ads_v5", "E8ads v5", "Eadsv5 Series"),
            ("Standard_E8as_v4", "E8a v4/E8as v4", "Eav4/Easv4 Series"),
            ("Standard_E8d_v4", "E8d v4", "Edv4 Series"),
            ("Standard_E8d_v5", "E8d v5", "Edv5 Series"),
            ("Standard_E8ds_v4", "E8ds v4", "Edsv4 Series"),
            ("Standard_E8ds_v5", "E8ds v5", "Edsv5 Series"),
            ("Standard_E8s_v3", "E8 v3/E8s v3", "Ev3/ESv3 Series"),
            ("Standard_E96-24ads_v5", "E96-24ads v5", "Eadsv5 Series"),
            ("Standard_E96-24as_v4", "E96-24as_v4", "Eav4/Easv4 Series"),
            ("Standard_E96-24ds_v5", "E96ds v5", "Edsv5 Series"),
            ("Standard_E96-48ads_v5", "E96-48ads v5", "Eadsv5 Series"),
            ("Standard_E96-48as_v4", "E96-48as_v4", "Eav4/Easv4 Series"),
            ("Standard_E96-48ds_v5", "E96ds v5", "Edsv5 Series"),
            ("Standard_E96a_v4", "E96a v4/E96as v4", "Eav4/Easv4 Series"),
            ("Standard_E96ads_v5", "E96ads v5", "Eadsv5 Series"),
            ("Standard_E96as_v4", "E96a v4/E96as v4", "Eav4/Easv4 Series"),
            ("Standard_E96d_v5", "E96d v5", "Edv5 Series"),
            ("Standard_E96ds_v5", "E96ds v5", "Edsv5 Series"),
            ("Standard_F16s_v2", "F16s v2", "FSv2 Series"),
            ("Standard_F2s_v2", "F2s v2", "FSv2 Series"),
            ("Standard_F32s_v2", "F32s v2", "FSv2 Series"),
            ("Standard_F48s_v2", "F48s v2", "FSv2 Series"),
            ("Standard_F4s_v2", "F4s v2", "FSv2 Series"),
            ("Standard_F64s_v2", "F64s v2", "FSv2 Series"),
            ("Standard_F72s_v2", "F72s v2", "FSv2 Series"),
            ("Standard_F8s_v2", "F8s v2", "FSv2 Series"),
            ("Standard_H16", "H16", "H Series"),
            ("Standard_H16_Promo", "H16", "H Promo Series"),
            ("Standard_H16m", "H16m", "H Series"),
            ("Standard_H16m_Promo", "H16m", "H Promo Series"),
            ("Standard_H16mr", "H16mr", "H Series"),
            ("Standard_H16mr_Promo", "H16mr", "H Promo Series"),
            ("Standard_H16r", "H16r", "H Series"),
            ("Standard_H16r_Promo", "H16r", "H Promo Series"),
            ("Standard_H8", "H8", "H Series"),
            ("Standard_H8_Promo", "H8", "H Promo Series"),
            ("Standard_H8m", "H8m", "H Series"),
            ("Standard_H8m_Promo", "H8m", "H Promo Series"),
            ("Standard_HB120-16rs_v2", "HB120rs v2", "HBSv2 Series"),
            ("Standard_HB120-16rs_v3", "HB120rs_v3", "HBrsv3 Series"),
            ("Standard_HB120-32rs_v2", "HB120rs v2", "HBSv2 Series"),
            ("Standard_HB120-32rs_v3", "HB120rs_v3", "HBrsv3 Series"),
            ("Standard_HB120-64rs_v2", "HB120rs v2", "HBSv2 Series"),
            ("Standard_HB120-64rs_v3", "HB120rs_v3", "HBrsv3 Series"),
            ("Standard_HB120-96rs_v2", "HB120rs v2", "HBSv2 Series"),
            ("Standard_HB120-96rs_v3", "HB120rs_v3", "HBrsv3 Series"),
            ("Standard_HB120rs_v2", "HB120rs v2", "HBSv2 Series"),
            ("Standard_HB120rs_v3", "HB120rs_v3", "HBrsv3 Series"),
            ("Standard_HB60-15rs", "HB60-15rs", "HBS Series"),
            ("Standard_HB60-30rs", "HB60-30rs", "HBS Series"),
            ("Standard_HB60-45rs", "HB60-45rs", "HBS Series"),
            ("Standard_HB60rs", "HB60rs", "HBS Series"),
            ("Standard_HC44-16rs", "HC44-16rs", "HCS Series"),
            ("Standard_HC44-32rs", "HC44-32rs", "HCS Series"),
            ("Standard_HC44rs", "HC44rs", "HCS Series"),
            ("Standard_L16s_v2", "L16s v2", "LSv2 Series"),
            ("Standard_L32s_v2", "L32s v2", "LSv2 Series"),
            ("Standard_L48s_v2", "L48s v2", "LSv2 Series"),
            ("Standard_L64s_v2", "L64s v2", "LSv2 Series"),
            ("Standard_L80s_v2", "L80s v2", "LSv2 Series"),
            ("Standard_L8s_v2", "L8s v2", "LSv2 Series"),
            ("Standard_M128", "M128s", "MS Series"),
            ("Standard_M128-32ms", "M128ms", "MS Series"),
            ("Standard_M128-64ms", "M128ms", "MS Series"),
            ("Standard_M128m", "M128ms", "MS Series"),
            ("Standard_M128ms", "M128ms", "MS Series"),
            ("Standard_M128s", "M128s", "MS Series"),
            ("Standard_M16-4ms", "M16ms", "MS Series"),
            ("Standard_M16-8ms", "M16ms", "MS Series"),
            ("Standard_M16ms", "M16ms", "MS Series"),
            ("Standard_M32-16ms", "M32ms", "MS Series"),
            ("Standard_M32-8ms", "M32ms", "MS Series"),
            ("Standard_M32ls", "M32ls", "MS Series"),
            ("Standard_M32ms", "M32ms", "MS Series"),
            ("Standard_M32ts", "M32ts", "MS Series"),
            ("Standard_M64", "M64s", "MS Series"),
            ("Standard_M64-16ms", "M64ms", "MS Series"),
            ("Standard_M64-32ms", "M64ms", "MS Series"),
            ("Standard_M64ls", "M64ls", "MS Series"),
            ("Standard_M64m", "M64ms", "MS Series"),
            ("Standard_M64ms", "M64ms", "MS Series"),
            ("Standard_M64s", "M64s", "MS Series"),
            ("Standard_M8-2ms", "M8ms", "MS Series"),
            ("Standard_M8-4ms", "M8ms", "MS Series"),
            ("Standard_M8ms", "M8ms", "MS Series"),
            ("Standard_NC12_Promo", "NC12", "NC Promo Series"),
            ("Standard_NC12s_v3", "NC12s v3", "NCSv3 Series"),
            ("Standard_NC16as_T4_v3", "NC16as T4 v3", "NCasv3 T4 Series"),
            ("Standard_NC24_Promo", "NC24", "NC Promo Series"),
            ("Standard_NC24r_Promo", "NC24r", "NC Promo Series"),
            ("Standard_NC24rs_v3", "NC24rs v3", "NCSv3 Series"),
            ("Standard_NC24s_v3", "NC24s v3", "NCSv3 Series"),
            ("Standard_NC4as_T4_v3", "NC4as T4 v3", "NCasv3 T4 Series"),
            ("Standard_NC6_Promo", "NC6", "NC Promo Series"),
            ("Standard_NC64as_T4_v3", "NC64as T4 v3", "NCasv3 T4 Series"),
            ("Standard_NC6s_v3", "NC6s v3", "NCSv3 Series"),
            ("Standard_NC8as_T4_v3", "NC8as T4 v3", "NCasv3 T4 Series"),
            ("Standard_NP10s", "NP10s", "NP Series"),
            ("Standard_NP20s", "NP20s", "NP Series"),
            ("Standard_NP40s", "NP40s", "NP Series"),
            ("Standard_NV12_Promo", "NV12", "NV Promo Series"),
            ("Standard_NV12s_v3", "NV12s v3", "NVSv3 Series"),
            ("Standard_NV16as_v4", "NV16as v4", "NVasv4 Series"),
            ("Standard_NV24_Promo", "NV24", "NV Promo Series"),
            ("Standard_NV24s_v3", "NV24s v3", "NVSv3 Series"),
            ("Standard_NV32as_v4", "NV32as v4", "NVasv4 Series"),
            ("Standard_NV48s_v3", "NV48s v3", "NVSv3 Series"),
            ("Standard_NV4as_v4", "NV4as v4", "NVasv4 Series"),
            ("Standard_NV6_Promo", "NV6", "NV Promo Series"),
            ("Standard_NV8as_v4", "NV8as v4", "NVasv4 Series")        };

        // TODO: Batch will provide an API for this in a future release of the client library
        private static IEnumerable<string> VerifiedVmSizes => new List<string>
        {
            "Standard_A2_v2",
            "Standard_A2m_v2",
            "Standard_A4_v2",
            "Standard_A4m_v2",
            "Standard_A8_v2",
            "Standard_A8m_v2",
            "Standard_D1_v2",
            "Standard_D11_v2",
            "Standard_D12_v2",
            "Standard_D13_v2",
            "Standard_D14_v2",
            "Standard_D15_v2",
            "Standard_D16_v3",
            "Standard_D16a_v4",
            "Standard_D16ads_v5",
            "Standard_D16as_v4",
            "Standard_D16d_v4",
            "Standard_D16d_v5",
            "Standard_D16ds_v4",
            "Standard_D16ds_v5",
            "Standard_D16s_v3",
            "Standard_D2_v2",
            "Standard_D2_v3",
            "Standard_D2a_v4",
            "Standard_D2ads_v5",
            "Standard_D2as_v4",
            "Standard_D2d_v4",
            "Standard_D2d_v5",
            "Standard_D2ds_v4",
            "Standard_D2ds_v5",
            "Standard_D2s_v3",
            "Standard_D3_v2",
            "Standard_D32_v3",
            "Standard_D32a_v4",
            "Standard_D32ads_v5",
            "Standard_D32as_v4",
            "Standard_D32d_v4",
            "Standard_D32d_v5",
            "Standard_D32ds_v4",
            "Standard_D32ds_v5",
            "Standard_D32s_v3",
            "Standard_D4_v2",
            "Standard_D4_v3",
            "Standard_D48_v3",
            "Standard_D48a_v4",
            "Standard_D48ads_v5",
            "Standard_D48as_v4",
            "Standard_D48d_v4",
            "Standard_D48d_v5",
            "Standard_D48ds_v4",
            "Standard_D48ds_v5",
            "Standard_D48s_v3",
            "Standard_D4a_v4",
            "Standard_D4ads_v5",
            "Standard_D4as_v4",
            "Standard_D4d_v4",
            "Standard_D4d_v5",
            "Standard_D4ds_v4",
            "Standard_D4ds_v5",
            "Standard_D4s_v3",
            "Standard_D5_v2",
            "Standard_D64_v3",
            "Standard_D64a_v4",
            "Standard_D64ads_v5",
            "Standard_D64as_v4",
            "Standard_D64d_v4",
            "Standard_D64d_v5",
            "Standard_D64ds_v4",
            "Standard_D64ds_v5",
            "Standard_D64s_v3",
            "Standard_D8_v3",
            "Standard_D8a_v4",
            "Standard_D8ads_v5",
            "Standard_D8as_v4",
            "Standard_D8d_v4",
            "Standard_D8d_v5",
            "Standard_D8ds_v4",
            "Standard_D8ds_v5",
            "Standard_D8s_v3",
            "Standard_D96a_v4",
            "Standard_D96ads_v5",
            "Standard_D96as_v4",
            "Standard_D96d_v5",
            "Standard_D96ds_v5",
            "Standard_DS1_v2",
            "Standard_DS11_v2",
            "Standard_DS12_v2",
            "Standard_DS13_v2",
            "Standard_DS14_v2",
            "Standard_DS15_v2",
            "Standard_DS2_v2",
            "Standard_DS3_v2",
            "Standard_DS4_v2",
            "Standard_DS5_v2",
            "Standard_E16_v3",
            "Standard_E16a_v4",
            "Standard_E16ads_v5",
            "Standard_E16as_v4",
            "Standard_E16d_v4",
            "Standard_E16d_v5",
            "Standard_E16ds_v4",
            "Standard_E16ds_v5",
            "Standard_E16s_v3",
            "Standard_E2_v3",
            "Standard_E20a_v4",
            "Standard_E20ads_v5",
            "Standard_E20as_v4",
            "Standard_E20d_v4",
            "Standard_E20d_v5",
            "Standard_E20ds_v4",
            "Standard_E20ds_v5",
            "Standard_E2a_v4",
            "Standard_E2ads_v5",
            "Standard_E2as_v4",
            "Standard_E2d_v4",
            "Standard_E2d_v5",
            "Standard_E2ds_v4",
            "Standard_E2ds_v5",
            "Standard_E2s_v3",
            "Standard_E32_v3",
            "Standard_E32a_v4",
            "Standard_E32ads_v5",
            "Standard_E32as_v4",
            "Standard_E32d_v4",
            "Standard_E32d_v5",
            "Standard_E32ds_v4",
            "Standard_E32ds_v5",
            "Standard_E32s_v3",
            "Standard_E4_v3",
            "Standard_E48_v3",
            "Standard_E48a_v4",
            "Standard_E48ads_v5",
            "Standard_E48as_v4",
            "Standard_E48d_v4",
            "Standard_E48d_v5",
            "Standard_E48ds_v4",
            "Standard_E48ds_v5",
            "Standard_E48s_v3",
            "Standard_E4a_v4",
            "Standard_E4ads_v5",
            "Standard_E4as_v4",
            "Standard_E4d_v4",
            "Standard_E4d_v5",
            "Standard_E4ds_v4",
            "Standard_E4ds_v5",
            "Standard_E4s_v3",
            "Standard_E64_v3",
            "Standard_E64a_v4",
            "Standard_E64ads_v5",
            "Standard_E64as_v4",
            "Standard_E64d_v4",
            "Standard_E64d_v5",
            "Standard_E64ds_v4",
            "Standard_E64ds_v5",
            "Standard_E64i_v3",
            "Standard_E64s_v3",
            "Standard_E8_v3",
            "Standard_E80ids_v4",
            "Standard_E8a_v4",
            "Standard_E8ads_v5",
            "Standard_E8as_v4",
            "Standard_E8d_v4",
            "Standard_E8d_v5",
            "Standard_E8ds_v4",
            "Standard_E8ds_v5",
            "Standard_E8s_v3",
            "Standard_E96a_v4",
            "Standard_E96ads_v5",
            "Standard_E96as_v4",
            "Standard_E96d_v5",
            "Standard_E96ds_v5",
            "Standard_F16s_v2",
            "Standard_F2s_v2",
            "Standard_F32s_v2",
            "Standard_F48s_v2",
            "Standard_F4s_v2",
            "Standard_F64s_v2",
            "Standard_F72s_v2",
            "Standard_F8s_v2",
            "Standard_HB120-16rs_v3",
            "Standard_HB120-32rs_v3",
            "Standard_HB120-64rs_v3",
            "Standard_HB120-96rs_v3",
            "Standard_HB120rs_v2",
            "Standard_HB120rs_v3",
            "Standard_HB60rs",
            "Standard_HC44rs",
            "Standard_L16s_v2",
            "Standard_L32s_v2",
            "Standard_L48s_v2",
            "Standard_L64s_v2",
            "Standard_L80s_v2",
            "Standard_L8s_v2",
            "Standard_M128",
            "Standard_M128m",
            "Standard_M128ms",
            "Standard_M128s",
            "Standard_M16ms",
            "Standard_M32ls",
            "Standard_M32ms",
            "Standard_M32ts",
            "Standard_M64",
            "Standard_M64ls",
            "Standard_M64m",
            "Standard_M64ms",
            "Standard_M64s",
            "Standard_M8ms",
            "Standard_NC16as_T4_v3",
            "Standard_NC4as_T4_v3",
            "Standard_NC64as_T4_v3",
            "Standard_NC6s_v3",
            "Standard_NC8as_T4_v3",
            "Standard_NV12s_v3",
            "Standard_NV24s_v3",
            "Standard_NV48s_v3"
        };
    }
}
