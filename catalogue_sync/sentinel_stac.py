# imported constants, should be updated by developers
s1_files = ["manifest.safe"]
s2_files = ["manifest.safe"]
s3_files = ["xfdumanifest.xml"]
s5_files = []
product_collection_mapping = {
    r'^S1[A-DP]_.._GRD[HM]_.*': 'sentinel-1-grd',
    r'^S1[A-DP]_.._SLC__.*': 'sentinel-1-slc',
    r'^S1[A-DP]_.._RAW__.*': 'sentinel-1-raw',
    r'^S1[A-DP]_.._OCN__.*': 'sentinel-1-ocn',
    r'^S2[A-DP]_MSIL1B_.*': 'sentinel-2-l1b',
    r'^S2[A-DP]_MSIL1C_.*': 'sentinel-2-l1c',
    r'^S2[A-DP]_MSIL2A_.*': 'sentinel-2-l2a',
    r'^S3[A-DP]_OL_1_.*': 'sentinel-3-olci-l1b',
    r'^S3[A-DP]_OL_2_.*': 'sentinel-3-olci-l2',
    r'^S3[A-DP]_SL_1_.*': 'sentinel-3-slstr-l1b',
    r'^S3[A-DP]_SL_2_.*': 'sentinel-3-slstr-l2',
    r'^S3[A-DP]_SR_1_.*': 'sentinel-3-stm-l1',
    r'^S3[A-DP]_SR_2_.*': 'sentinel-3-stm-l2',
    r'^S3[A-DP]_SY_1_.*': 'sentinel-3-syn-l1',
    r'^S3[A-DP]_SY_2_.*': 'sentinel-3-syn-l2',
    r'^S5[A-DP]_OFFL_L1_.*': 'sentinel-5p-l1',
    r'^S5[A-DP]_NRTI_L1_.*': 'sentinel-5p-l1',
    r'^S5[A-DP]_OFFL_L2_.*': 'sentinel-5p-l2',
    r'^S5[A-DP]_NRTI_L2_.*': 'sentinel-5p-l2',
}
