# CreatedBy: Emilia Crow
# CreateDate: 20210526
# Updated: 20210805
# CreateFor: Franklin Young International


class HeaderTranslator():
    def __init__(self):
        self.name = 'Translator Teddy'
        # I would prefer this get read from elsewhere so it doesn't live in code
        self.dct_headers = {'VendorName':['VendorName','Vendor Name','primary_vendor_name'],

                            'PrimaryVendorPartNumber':['PrimaryVendorPartNumber'],
                            'PrimaryVendorListPrice':['PrimaryVendorListPrice'],
                            'PrimaryFyCost':['PrimaryFyCost'],
                            'PrimaryFyDiscountPercent':['PrimaryFyDiscountPercent'],
                            'PrimaryEstimatedFreight':['PrimaryEstimatedFreight'],

                            'SecondaryVendorPartNumber':['SecondaryVendorPartNumber'],
                            'SecondaryVendorListPrice':['SecondaryVendorListPrice'],
                            'SecondaryVendorFyCost':['SecondaryVendorFyCost'],
                            'SecondaryFOBOrigin':['SecondaryFOBOrigin'],
                            'FOBOrigin':['FOBOrigin'],
                            'DirectVendorCheck':['DirectVendorCheck'],

                            'IsFeaturedProduct':['IsFeaturedProduct'],
                            'VendorFyProductNumber':['VendorFyProductNumber'],
                            'PossibleVendorPartNumber':['PossibleVendorPartNumber'],

                            'TAA Compliant':['TAA Compliant'],


                            'VendorCode':['VendorCode', 'Vendor Code', 'primary_vendor_code'],
                            'VendorPartNumber':['VendorPartNumber','Vendor Part Number','primary_vendor_part_no','Thomas Item #','vendor_part_no'],

                            'ManufacturerName':['ManufacturerName','Manufacturer Name','Manu Name','manufacturer_name','Manufacturer Standard Name'],
                            'SupplierName':['SupplierName','Supplier Name','supplier name','supplier_name'],

                            'ManufacturerPartNumber':['GPS Manufacturer Part Number','ManufacturerPartNumber','manufacturer_part_number','manufacturer_part_no','Manufacturer Part Number','Manu Part #','MFR/Supplier Part#','Supplier Part#','SupplierPartNumber','manufacturer_part_no','Supplier Part # '],

                            'CountryOfOrigin':['CountryOfOrigin','CountryCode','VendorCountryCode','VendorCountryOfOrigin','Country of Origin', 'Country Of Origin','COO','Country','country_of_origin_code','country_of_origin'],

                            'VendorProductName':['VendorProductName', 'Vendor Product Name', 'vendor_product_name',
                                                 'primary_vendor_product_name', 'ProductName', 'Product Name',
                                                 'product_name', 'prodname'],
                            'VendorProductDescription':['VendorProductDescription', 'VendorDescription','Vendor Product Description',
                                                        'vendor_product_description', 'VendorProductDesc',
                                                        'Vendor Product Desc', 'vendor_product_desc',
                                                        'ProductDescription', 'Product Description',
                                                        'product_description',
                                                        'ProductDesc', 'Product Desc', 'product_desc',
                                                        'LongDescription', 'Long Description', 'long_description',
                                                        'long_desc', 'LongDesc', 'Long Desc'],
                            'ShortDescription':['ShortDescription', 'Short Description', 'short_description',
                                                'short_desc', 'ShortDesc', 'Short Desc'],
                            'ECommerceLongDescription':['ECommerceLongDescription', 'ECommerce Long Description',
                                                        'ecommerce_long_description'],

                            'UnitOfIssueQuantity':['UnitOfIssueQuantity','Conv Factor/QTY UOM', 'Conv Factor', 'Conversion Factor','UnitOfIssueCount', 'Unit Of Issue Count','unit_of_issue_qty','quantity','Quantity','unit_of_issue_qty_1','UOIQuantity','Quantity per Unit of Issue','QtyUOI','Qty UOI','Qty','UnitOfIssueByQuantity'],
                            'UnitOfIssue':['UnitOfIssue', 'Unit Of Issue', 'UnitOfIssueSymbol', 'Unit Of Issue Symbol', 'UOI', 'uoi','unit_of_issue','unit of issue'],
                            'UnitOfMeasure':['UnitOfMeasure','Unit Of Measure','UnitOfMeasureSymbol','Unit Of Measure Symbol','UOM','uom','unit_of_measure','unit of measure','uom_std','UOM - STD','Unit code for Pack Quantity'],

                            'IsHazardous': ['IsHazardous','Is Hazardous', 'Hazardous', 'is_hazardous','VendorIsHazardous','Vendor Is Hazardous', 'Vendor Hazardous', 'vendor_is_hazardous'],
                            'FyIsHazardous':['FyIsHazardous','fy_is_hazardous','FyIsHazardous','Fy Is Hazardous', 'Fy Hazardous', 'fy_is_hazardous'],
                            'HazardCode':['HazardCode','Hazard Code','Hazardous Code'],
                            'LeadTimeDays':['LeadTimeDays', 'LeadTimes','LeadTime','LeadTimeDays','Lead Time Days','Lead Time','lead_time_days', 'Lead Times', 'Lead Time', 'lead_time','VendorLeadTimeDays', 'VendorLeadTimes', 'VendorLeadTime', 'VendorLeadTimeDays', 'Vendor Lead Time Days','Vendor Lead Time', 'vendor_lead_time_days', 'Vendor Lead Times', 'Vendor Lead Time', 'vendor_lead_time'],
                            'LeadTimeExpedited':['LeadTimeDaysExpedited','Lead Time Days Expedited','Lead Time Expedited','expedited_lead_time_days','expedited_lead_time'],

                            'VendorListPrice':['Vendor List Price','VendorListPrice','VendorList','Vendor List','List Price','primary_vendor_list_price','vendor_list_price'],
                            'Discount':['Discount','VendorDiscount','Vendor Discount','Vendor Discount to FY','FyDiscountPercent','FyDiscByVendor'],
                            'FyCost':['Cost Price','Your Price','FyCost','Fy Cost','primary_vendor_fy_cost'],
                            'EstimatedFreight':['EstimatedFrieght','Estimated Freight','EstimatedFreight','Estimated Shipping','EstimatedShipping','primary_vendor_estimated_freight($)'],
                            'VendorProductNotes':['ProductNotes', 'Product Notes','VendorProductNotes','Vendor Product Notes'],

                            'ECommerceDiscount':['ECommerceDiscount'],
                            'FySellGrossMargin':['FySellGrossMargin'],
                            'FySellGrossMarginPercent':['FySellGrossMarginPercent'],
                            'DefaultImageId':['DefaultImageId'],

                            'ProductSortOrder':['ProductSortOrder','Product Sort Order','FyProductSortOrder','Fy Product Sort Order'],

                            'WebsiteOnly':['WebsiteOnly', 'Website Only', 'Web Only'],
                            'GSAEligible':['GSAEligible', 'GSA Eligible', 'GSA Is Eligible', 'GSA IsEligible'],
                            'VAEligible':['VAEligible', 'VA Eligible', 'VA Is Eligible', 'VA IsEligible'],
                            'ECATEligible':['ECATEligible', 'ECAT Eligible', 'ECAT Is Eligible', 'ECAT IsEligible'],
                            'HTMEEligible':['HTMEEligible', 'HTME Eligible', 'HTME Is Eligible', 'HTME IsEligible'],
                            'INTRAMALLSEligible':['INTRAMALLSEligible', 'INTRAMALLS Eligible', 'INTRAMALLS Is Eligible', 'INTRAMALLS IsEligible'],
                            'VendorIsDiscontinued':['IsDiscontinued', 'VendorIsDiscontinued', 'Discontinued', 'is discontinued','Is Discontinued', 'is_discontinued', 'vendor_is_discontinued', 'Vendor_Is_Discontinued'],


                            ## FyDeclaredValues
                            'FyCatalogNumber':['FyCatalogNumber', 'FyCatalog#', 'FYCatalogNumber', 'Fy Catalog Number',
                                               'FY Catalog Number'],
                            'FyProductNumber':['FyProductNumber', 'FyProduct#', 'FYProductNumber', 'Fy Product Number',
                                               'FY Product Number'],
                            'PrimaryVendorName':['PrimaryVendorName','PrimaryVendorCode', 'Primary Vendor Code', 'primary_vendor_code','PrimaryVendor','Primary Vendor'],
                            'SecondaryVendorName':['SecondaryVendorName','SecondaryVendorCode', 'Secondary Vendor Code', 'secondary_vendor_code','SecondaryVendor','Secondary Vendor'],
                            'FyManufacturerPartNumber':['FyManufacturerPartNumber','ContractedManufacturerPartNumber','Contracted Manufacturer Part Number','ContractedManufacturerNumber','Contracted Manufacturer Number','ContractedPartNumber', 'Contracted Part Number'],

                            'FyPartNumber':['FYPartNumber', 'FyPartNumber', 'fy_part_number', 'fy_part_no', 'Product Code/SKU'],
                            'FyCategory':['FyCategory','CategoryRecommendation', 'Category', 'category'],
                            'FyProductName':['FyProductName', 'Fy Product Name','Fy ProductName'],
                            'FyProductDescription':['FyProductDesctription','FyProductDescription', 'Fy Product Description', 'Fy ProductDescription','FyProductDesc', 'Fy Product Desc', 'Fy ProductDesc'],
                            'FyCountryOfOrigin':['FY_CountryOfOrigin', 'FyCountryOfOrigin', 'Fy Country of Origin', 'Fy CountryOfOrigin','FyCOO', 'Fy COO', 'Fy_COO','FyCountryCode','Fy Country Code'],

                            'FyUnitOfIssue':['FY_UOI', 'FyUnitOfIssue', 'Fy Unit Of Issue', 'Fy UnitOfIssue','FyUOI', 'Fy UOI', 'Fy_UOI'],
                            'FyUnitOfIssueQuantity':['FY_UOIQTY', 'FyUOIQTY', 'FyUnitOfIssueQuantity', 'FyUnitOfIssueByQuantity', 'Fy Unit Of Issue Quantity', 'Fy Unit Of Issue By Quantity', 'Fy UOIQuantity','Fy_UOIQuantity','FyUOIQuantity','Fy UnitOfIssueQuantity','FyQTY', 'Fy QTY', 'Fy_QTY'],
                            'FyUnitOfMeasure':['FY_UOM', 'FyUnitOfMeasure', 'Fy Unit Of Measure', 'Fy UnitOfMeasure', 'FyUOM',
                                             'Fy UOM', 'Fy_UOM'],

                            'RecommendedStorage':['shelf_life_months','RecommendedStorage','StorageInstruction'],

                            'FyLandedCost':['FyLandedCost','LandedCost','Fy Landed Cost','Landed Cost'],
                            'FyLandedCostMarkupPercent_FySell':['FyLandedCostMarkupPercent_FYSell', 'FyLandedCostMarkupPercent_Sell', 'FyLandedCostMarkupPercent_FySell', 'LandedCostMarkupPercent_FYSell', 'LandedCostMarkupPercent_FySell', 'Markup', 'M/U Ron', 'MarkUp Ron', 'Mark Up Ron','M/U Ron'],
                            'FySellPrice':['FySellPrice','FyPrice','BC Sell Price','Ecom Sell Price'],
                            'FyLandedCostMarkupPercent_FyList':['FyLandedCostMarkupPercent_FYList','FyLandedCostMarkupPercent_List','FyLandedCostMarkupPercent_FyList','LandedCostMarkupPercent_FYList','LandedCostMarkupPercent_FyList','M/U Linda','MarkUp Linda','Mark Up Linda','M/U Linda','landed_cost_mark_up(%)'],
                            'FyListPrice':['Retail Price','FyListPrice','FyList'],

                            'FyProductNotes':['FyProductNotes', 'Fy Product Notes', 'InternalProductNotes', 'Internal Product Notes', 'FYProductNotes', 'FY Product Notes'],

                            'FyLeadTimes':['FyLeadTimes','Fy_lead_time', 'FyLeadTime', 'Fy Lead Time', 'Fy LeadTime', 'fy lead time', 'fy lead_time', 'fy_lead_time','Fy_lead_times', 'FyLeadTimes', 'Fy Lead Times', 'Fy LeadTimes', 'fy lead times', 'fy lead_times', 'fy_lead_times'],

                            'FyDenyGSAContract':['FyDenyGSAContract','DenyGSAContract','Deny GSA Contract','Deny GSA','Fy Deny GSA Contract','Fy Deny GSA'],
                            'FyDenyGSAContractDate':['FyDenyGSAContractDate','DenyGSAContractDate','Deny GSA Contract Date','Deny GSA Date','Fy Deny GSA Contract Date','Fy Deny GSA Date'],
                            'FyDenyVAContract':['FyDenyVAContract','DenyVAContract','Deny VA Contract','Deny VA','Fy Deny VA Contract','Fy Deny VA'],
                            'FyDenyVAContractDate':['FyDenyVAContractDate','DenyVAContractDate','Deny VA Contract Date','Deny VA Date','Fy Deny VA Contract Date','Fy Deny VA Date'],
                            'FyDenyECATContract':['FyDenyECATContract','DenyECATContract','Deny ECAT Contract','Deny ECAT','Fy Deny ECAT Contract','Fy Deny ECAT'],
                            'FyDenyECATContractDate':['FyDenyECATContractDate','DenyECATContractDate','Deny ECAT Contract Date','Deny ECAT Date','Fy Deny ECAT Contract Date','Fy Deny ECAT Date'],
                            'FyDenyHTMEContract':['FyDenyHTMEContract','DenyHTMEContract','Deny HTME Contract','Deny HTME','Fy Deny HTME Contract','Fy Deny HTME'],
                            'FyDenyHTMEContractDate':['FyDenyHTMEContractDate','DenyHTMEContractDate','Deny HTME Contract Date','Deny HTME Date','Fy Deny HTME Contract Date','Fy Deny HTME Date'],
                            'FyDenyINTRAMALLSContract':['FyDenyINTRAMALLSContract', 'DenyINTRAMALLSContract', 'Deny INTRAMALLS Contract',
                                                  'Deny INTRAMALLS', 'Fy Deny INTRAMALLS Contract', 'Fy Deny INTRAMALLS'],
                            'FyDenyINTRAMALLSContractDate':['FyDenyINTRAMALLSContractDate', 'DenyINTRAMALLSContractDate',
                                                      'Deny INTRAMALLS Contract Date', 'Deny INTRAMALLS Date',
                                                      'Fy Deny INTRAMALLS Contract Date', 'Fy Deny INTRAMALLS Date'],

                            'FyIsDiscontinued':['FyIsDiscontinued', 'fy is discontinued', 'Fy Is Discontinued',
                                                'fy_is_discontinued', 'Fy_Is_Discontinued'],
                            'FyIsVisible':['IsVisible', 'Is Visible', 'is_visible', 'is visible', 'FyIsVisible',
                                           'Fy Is Visible', 'fy_is_visible', 'fy is visible'],
                            'FyAllowPurchases':['AllowPurchases', 'Allow Purchases', 'Allow Purchases?',
                                                'FyAllowPurchases', 'Fy Allow Purchases', 'Fy Allow Purchases?'],

                            'DateCatalogReceived':['DateCatalogRecieved', 'DateCatelogReceived', 'DateCatalogReceived',
                                                   'Date Catalog Recieved', 'date_catalog_received','DateCatalogRcvd'],
                            'CatalogProvidedBy':['CatalogProvidedBy','Catalog Provided By'],

                            'ProductTaxClass':['ProductTaxClass','Product Tax Class','Tax Class'],

                            'ImageUrl':['ImageUrl', 'ProductImageUrl', 'Image Url', 'Product Image Url',
                                        'product_photo_url'],
                            'ImageCaption':['ImageCaption','Image Caption','ImageDescription','Image Description','ImageName','ProductImageName','Image Name','Product Image Name','product_photo_Name'],
                            'ProductUrl':['URL','url','Product URL','Product Url','ProductUrl','product_url'],

                            'BCPriceUpdateToggle':['BCPriceUpdateToggle','BCPriceToggle','BC Price Update Toggle','BC Price Toggle'],
                            'BCDataUpdateToggle':['BCDataUpdateToggle','BCDataToggle','BC Data Update Toggle','BC Data Toggle'],
                            'AVInclusionToggle':['AVInclusionToggle','AVToggle','AV Inclusion Toggle','AV Toggle'],


                            # contract values
                            'ECATProductNotes':['ECATProductNotes', 'ECAT Product Notes', 'ECAT ProductNotes',
                                               'ECAT Product Notes'],
                            'GSAProductNotes':['GSAProductNotes', 'GSA Product Notes', 'GSA ProductNotes', 'GSA Product Notes'],
                            'HTMEProductNotes':['HTMEProductNotes', 'HTME Product Notes', 'HTME ProductNotes',
                                               'HTME Product Notes'],
                            'INTRAMALLSProductNotes':['INTRAMALLSProductNotes', 'INTRAMALLS Product Notes', 'INTRAMALLS ProductNotes',
                                               'INTRAMALLS Product Notes'],
                            'VAProductNotes':['VAProductNotes', 'VA Product Notes', 'VA ProductNotes',
                                               'VA Product Notes'],

                            'ECATOnContract':['ECATOnContract', 'ECAT On Contract', 'ECAT On Contract'],
                            'GSAOnContract':['GSAOnContract', 'GSA On Contract', 'GSA On Contract'],
                            'HTMEOnContract':['HTMEOnContract', 'HTME On Contract', 'HTME On Contract'],
                            'INTRAMALLSOnContract':['INTRAMALLSOnContract', 'INTRAMALLS On Contract', 'INTRAMALLS On Contract'],
                            'VAOnContract':['VAOnContract', 'VA On Contract', 'VA On Contract'],

                            'GSAApprovedGrossMarginPercent':['GSAApprovedGrossMarginPercent'],
                            'GSAProposedGrossMarginPercent':['GSAProposedGrossMarginPercent'],
                            'VAApprovedGrossMarginPercent':['VAApprovedGrossMarginPercent'],
                            'VAProposedGrossMarginPercent':['VAProposedGrossMarginPercent'],
                            'ECATApprovedGrossMarginPercent':['ECATApprovedGrossMarginPercent'],
                            'ECATProposedGrossMarginPercent':['ECATProposedGrossMarginPercent'],
                            'INTRAMALLSApprovedGrossMarginPercent':['INTRAMALLSApprovedGrossMarginPercent'],
                            'INTRAMALLSProposedGrossMarginPercent':['INTRAMALLSProposedGrossMarginPercent'],

                            'ECATApprovedListPrice':['ECATApprovedListPrice', 'ECATApprovedFYListPrice__value',
                                                     'ECATApprovedFYListPrice',
                                                     'ECATApprovedFYListPrice'],
                            'ECATDiscountPercent':['ECATApprovedPercent', 'ECATChannelDiscount', 'ECATDiscount',
                                                   'ECATDiscountPercent', 'ECAT Channel Discount',
                                                   'ECAT Discount', 'ecat_channel_discount', 'ECAT Approved Channel%'],
                            'ECATApprovedLandedCost':['ECATApprovedLandedCost', 'ECATLandedCost', 'ECAT Landed Cost'],
                            'ECATApprovedBasePrice':['ECATApprovedBasePrice', 'ECATBasePrice', 'ECAT Base Price'],
                            'ECATApprovedSellPrice':['ECATApprovedSellPrice', 'ECATSellPrice', 'ECAT Sell Price'],
                            'ECATContractNumber':['ECATContractNumber'],
                            'ECATPricingApproved':['ECATPricingApproved'],
                            'ECATContractModificationNumber':['ECATContractModificationNumber',
                                                              'ECAT Contract Modification Number',
                                                              'ECATContractModification', 'ECAT Contract Modification',
                                                              'ECATModificationNumber', 'ECAT Modification Number'],
                            'ECATApprovedPriceDate':['ECATApprovedPriceDate', 'ECAT Approved Price Date',
                                                     'ECAT Approved Date'],
                            'ECATMaxMarkup':['ECATMaxMarkup', 'ECAT MaxMarkup',
                                             'ECAT Max Markup'],

                            'GSAApprovedListPrice':['GSAApprovedListPrice', 'GSAApprovedFYListPrice__value',
                                                    'GSAApprovedFYListPrice',
                                                    'GSAApprovedFYListPrice'],
                            'GSADiscountPercent':['GSAApprovedPercent', 'GSAChannelDiscount', 'GSADiscount',
                                                  'GSA Channel Discount',
                                                  'GSA Discount', 'gsa_channel_discount', 'GSA Approved Channel%',
                                                  'GSADiscountPercent', 'GSA disc%'],
                            'GSAApprovedBasePrice':['GSABasePrice', 'GSA Base Price', 'GSAApprovedBasePrice'],
                            'GSAApprovedSellPrice':['GSASellPrice', 'GSA Sell Price', 'current_gsa_price',
                                                    'GSAApprovedSellPrice'],
                            'GSAContractNumber':['GSAContractNumber'],
                            'GSAPricingApproved':['GSAPricingApproved'],
                            'GSAContractModificationNumber':['GSAContractModificationNumber', 'GSAModificationNumber',
                                                             'GSA Contract Modification Number',
                                                             'GSAContractModification', 'GSA Contract Modification',
                                                             'GSA Modification Number'],
                            'GSAApprovedPriceDate':['GSAApprovedPriceDate', 'GSA Approved Price Date',
                                                    'GSA Approved Date'],
                            'GSA_Sin':['GSA_Sin', 'GSA_SIN', 'GSA Sin', 'GSA SIN', 'GSASin', 'GSA Sin #', 'GSASin#'],

                            # intramalls
                            'INTRAMALLSApprovedBasePrice':['INTRAMALLSBasePrice', 'INTRAMALLS Base Price','INTRAMALLSApprovedBasePrice'],
                            'INTRAMALLSApprovedSellPrice':['INTRAMALLSSellPrice', 'INTRAMALLS Sell Price','current_intramalls_price','INTRAMALLSApprovedSellPrice'],
                            'INTRAMALLSApprovedListPrice':['INTRAMALLSApprovedListPrice','INTRAMALLSApprovedFYListPrice__value', 'INTRAMALLSApprovedFYListPrice',
                                                    'INTRAMALLSApprovedFYListPrice'],
                            'INTRAMALLSContractNumber':['INTRAMALLSContractNumber'],
                            'INTRAMALLSPricingApproved':['INTRAMALLSPricingApproved'],
                            'INTRAMALLSContractModificationNumber':['INTRAMALLSContractModificationNumber','INTRAMALLSModificationNumber',
                                                             'INTRAMALLS Contract Modification Number',
                                                             'INTRAMALLSContractModification', 'INTRAMALLS Contract Modification','INTRAMALLS Modification Number'],
                            'INTRAMALLSApprovedPriceDate':['INTRAMALLSApprovedPriceDate', 'INTRAMALLS Approved Price Date',
                                                    'INTRAMALLS Approved Date'],
# VA
                            'VAApprovedListPrice':['ChannelApprovedFYListPrice__value', 'ChannelApprovedFYListPrice',
                                                    'ApprovedFYListPrice'],
                            'VADiscountPercent':['VAApprovedPercent','VAChannelDiscount', 'VADiscount', 'VA Channel Discount',
                                                  'VA Discount', 'va_channel_discount', 'VA Approved Channel%','VADiscountPercent'],
                            'VAApprovedBasePrice':['VAApprovedBasePrice','VABasePrice', 'VA Base Price'],
                            'VAApprovedSellPrice':['VAApprovedSellPrice','VASellPrice', 'VA Sell Price'],
                            'VAContractNumber':['VAContractNumber'],
                            'VAPricingApproved':['VAPricingApproved'],
                            'VAContractModificationNumber':['VAContractModificationNumber',
                                                             'VA Contract Modification Number',
                                                             'VAContractModification', 'VA Contract Modification','VAModificationNumber','VA Modification Number'],
                            'VAApprovedPriceDate':['VAApprovedPriceDate', 'VA Approved Price Date',
                                                    'VA Approved Date'],

                            'VA_Sin':['VA_Sin','VA_SIN','VA Sin','VA SIN','VASin','VA Sin #','VASin#'],
# ECAT

                            'VAMfcDiscountPercent':['VAMfcDiscountPercent', 'VA Mfc Discount Percent', 'va_mfc_disc(%)',
                                                   'approved VA MFC per','VA_MFC_Discount','VA MFC %'],
                            'GSAMfcDiscountPercent':['GSAMfcDiscountPercent', 'GSA Mfc Discount Percent', 'gsa_mfc_disc(%)',
                                                   'approved GSA MFC per','GSA_MFC_Discount','GSA MFC %'],

                            'MfcPrice':['MfcPrice', 'Approved MFC Price', 'Mfc Price'],

                            'ChangedDate':['ChangedDate'],
                            'ChangedBy':['ChangedBy'],

                            'FyProductNumberOverride':['FyProductNumberOverride', 'Fy Product Number Override','IsProductNumberOverride', 'ProductNumberOverride', 'Product Number Override'],

                            'ProductId':['ProductId'],
                            'ProductPriceId':['ProductPriceId'],
                            'PrimaryProductPriceId':['PrimaryProductPriceId'],
                            'SecondaryProductPriceId':['SecondaryProductPriceId'],
                            'ProductDescriptionId':['ProductDescriptionId'],

                            'Pass':['Pass'],
                            'Alert':['Alert'],
                            'Fail':['Fail'],
                            'ProductDescriptionId':['ProductDescriptionId'],'VendorId':['VendorId'],

                            'ManufacturerId':['ManufacturerId'],
                            'UpdateManufacturerName':['UpdateManufacturerName'],
                            'BaseProductPriceId':['BaseProductPriceId'],
                            'db_IsDiscontinued':['db_IsDiscontinued'],
                            'db_FyIsDiscontinued':['db_FyIsDiscontinued'],
                            'ECATProductPriceId':['ECATProductPriceId'],
                            'GSAProductPriceId':['GSAProductPriceId'],
                            'HTMEProductPriceId':['HTMEProductPriceId'],
                            'INTRAMALLSProductPriceId':['INTRAMALLSProductPriceId'],
                            'VAProductPriceId':['VAProductPriceId'],

                            'Filter':['Filter'],
                            'TakePriority':['TakePriority'],
                            'db_ProductNumberOverride':['db_ProductNumberOverride'],
                            'db_FyProductNotes':['db_FyProductNotes'],
                            'db_ECATProductNotes':['db_ECATProductNotes'],
                            'db_GSAProductNotes':['db_GSAProductNotes'],
                            'db_HTMEProductNotes':['db_HTMEProductNotes'],
                            'db_INTRAMALLSProductNotes':['db_INTRAMALLSProductNotes'],
                            'db_VAProductNotes':['db_VAProductNotes'],

                            'CategoryId':['CategoryId'],
                            'RecommendedStorageId':['RecommendedStorageId'],
                            'CountryOfOriginId':['CountryOfOriginId'],
                            'MinimumOrderQty':['MinimumOrderQty'],
                            'FyCountryOfOriginId':['FyCountryOfOriginId'],
                            'ExpectedLeadTimeId':['ExpectedLeadTimeId'],
                            'IsFreeShipping':['IsFreeShipping'],
                            'IsColdChain':['IsColdChain'],
                            'ShippingInstructionsId':['ShippingInstructionsId'],
                            'db_FyProductName':['db_FyProductName'],
                            'db_FyProductDescription':['db_FyProductDescription'],
                            'FyUnitOfIssueSymbolId':['FyUnitOfIssueSymbolId'],
                            'UnitOfIssueSymbolId':['UnitOfIssueSymbolId'],
                            'UnitOfMeasureSymbolId':['UnitOfMeasureSymbolId'],
                            'PrimaryVendorId':['PrimaryVendorId'],
                            'SecondaryVendorId':['SecondaryVendorId'],
                            'AssetPath':['AssetPath'],
                            'AssetType':['AssetType'],
                            'UpdateAssets':['UpdateAssets'],
                            'AssetPreference':['AssetPreference'],
                            'CurrentFyProductNumber':['CurrentFyProductNumber'],
                            'PossibleVendorName':['PossibleVendorName'],
                            'BlockedManufacturer':['BlockedManufacturer'],
                            }
        # that's stupid
        self.build_trans_dct()

    def build_trans_dct(self):
        self.translator_dict = {}
        for correct_head in self.dct_headers:
            for origin_head in self.dct_headers[correct_head]:
                self.translator_dict[origin_head.lower()] = correct_head


    def translate_headers(self,lst_in_heads):
        lst_out_heads = []
        lst_unmatched_headers = []
        for each_header in lst_in_heads:
            clean_head = str(each_header).strip()
            clean_head = clean_head.lower()

            if clean_head in self.translator_dict:
                if self.translator_dict[clean_head] not in lst_out_heads:
                    lst_out_heads.append(self.translator_dict[clean_head])
                else:
                    lst_out_heads.append(each_header)
                    lst_unmatched_headers.append(each_header)

            else:
                lst_out_heads.append(each_header)
                lst_unmatched_headers.append(each_header)

        return lst_out_heads, lst_unmatched_headers


## end ##