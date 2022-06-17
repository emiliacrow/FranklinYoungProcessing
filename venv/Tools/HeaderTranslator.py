# CreatedBy: Emilia Crow
# CreateDate: 20210526
# Updated: 20210805
# CreateFor: Franklin Young International


class HeaderTranslator():
    def __init__(self):
        self.name = 'Translator Teddy'
        # I would prefer this get read from elsewhere so it doesn't live in code
        # that's stupid
        self.dct_headers = {'VendorName':['VendorName','Vendor Name','primary_vendor_name'],
                            'VendorCode':['VendorCode', 'Vendor Code', 'primary_vendor_code'],
                            'ManufacturerName':['ManufacturerName','Manufacturer Name','Manu Name','manufacturer_name','Manufacturer Standard Name'],
                            'SupplierName':['SupplierName','Supplier Name','supplier name','supplier_name'],

                            'VendorPartNumber':['VendorPartNumber','Vendor Part Number','primary_vendor_part_no','Thomas Item #','vendor_part_no'],
                            'CountryOfOrigin':['CountryOfOrigin','CountryCode','Country of Origin', 'Country Of Origin','COO','Country','country_of_origin_code','country_of_origin'],

                            'ManufacturerPartNumber':['GPS Manufacturer Part Number','ManufacturerPartNumber','manufacturer_part_number','manufacturer_part_no','Manufacturer Part Number','Manu Part #','MFR/Supplier Part#','Supplier Part#','SupplierPartNumber','manufacturer_part_no','Supplier Part # '],
                            'ContractedManufacturerPartNumber':['ContractedManufacturerPartNumber','Contracted Manufacturer Part Number','ContractedManufacturerNumber','Contracted Manufacturer Number','ContractedPartNumber','Contracted Part Number'],
                            'FyProductNumber':['FyProductNumber','FyProduct#', 'FYProductNumber', 'Fy Product Number', 'FY Product Number'],
                            'FyCatalogNumber':['FyCatalogNumber','FyCatalog#', 'FYCatalogNumber', 'Fy Catalog Number', 'FY Catalog Number'],
                            'FyPartNumber':['FYPartNumber', 'FyPartNumber', 'fy_part_number', 'fy_part_no', 'Product Code/SKU'],
                            'RecommendedStorage':['shelf_life_months','RecommendedStorage','StorageInstruction'],

                            'ProductName':['ProductName','Product Name','product_name','vendor_product_name','primary_vendor_product_name'],
                            'ProductDescription':['ProductDescription','Product Description','product_description','ProductDesc','Product Desc','product_desc', 'prodname'],
                            'ShortDescription':['ShortDescription','Short Description','short_description','short_desc','ShortDesc','Short Desc'],
                            'LongDescription':['LongDescription','Long Description','long_description','long_desc','LongDesc','Long Desc'],
                            'ECommerceLongDescription':['ECommerceLongDescription','ECommerce Long Description','ecommerce_long_description'],

                            'Conv Factor/QTY UOM':['Conv Factor/QTY UOM', 'Conv Factor', 'Conversion Factor','UnitOfIssueCount', 'Unit Of Issue Count','unit_of_issue_qty','quantity','Quantity','unit_of_issue_qty_1','UOIQuantity','Quantity per Unit of Issue','QtyUOI','Qty UOI','Qty','UnitOfIssueByQuantity'],
                            'UnitOfIssue':['UnitOfIssue', 'Unit Of Issue', 'UnitOfIssueSymbol', 'Unit Of Issue Symbol', 'UOI', 'uoi','unit_of_issue','unit of issue'],
                            'UnitOfMeasure':['UnitOfMeasure','Unit Of Measure','UnitOfMeasureSymbol','Unit Of Measure Symbol','UOM','uom','unit_of_measure','unit of measure','uom_std','UOM - STD','Unit code for Pack Quantity'],
                            'IsHazardous':['IsHazardous','Is Hazardous','Hazardous','is_hazardous'],
                            'HazardCode':['HazardCode','Hazard Code','Hazardous Code'],
                            'IsVisible':['IsVisible','Is Visible','is_visible','is visible'],
                            'LeadTime':['LeadTime','LeadTimeDays','Lead Time Days','Lead Time','lead_time_days', 'Lead Times', 'Lead Time', 'lead_time'],
                            'LeadTimeExpedited':['LeadTimeDaysExpedited','Lead Time Days Expedited','Lead Time Expedited','expedited_lead_time_days','expedited_lead_time'],

                            'VendorListPrice':['Vendor List Price','VendorListPrice','VendorList','Vendor List','List Price','primary_vendor_list_price','vendor_list_price'],
                            'Discount':['Discount','VendorDiscount','Vendor Discount','Vendor Discount to FY','FyDiscountPercent','FyDiscByVendor'],
                            'FyCost':['Cost Price','Your Price','FyCost','Fy Cost','primary_vendor_fy_cost'],
                            'Estimated Freight':['Estimated Freight','EstimatedFreight','Estimated Shipping','EstimatedShipping','primary_vendor_estimated_freight($)'],
                            'Landed Cost':['FyLandedCost','LandedCost','Fy Landed Cost','Landed Cost'],
                            'LandedCostMarkupPercent_FYSell':['LandedCostMarkupPercent_FYSell', 'LandedCostMarkupPercent_FySell', 'Markup', 'M/U Ron', 'MarkUp Ron', 'Mark Up Ron','M/U Ron'],
                            'Sell Price':['FySellPrice','FyPrice','BC Sell Price','Ecom Sell Price'],
                            'LandedCostMarkupPercent_FYList':['LandedCostMarkupPercent_FYList','LandedCostMarkupPercent_FyList','M/U Linda','MarkUp Linda','Mark Up Linda','M/U Linda','landed_cost_mark_up(%)'],
                            'Retail Price':['Retail Price','FyListPrice','FyList'],

                            'OnContract':['OnContract', 'On Contract'],
                            'FyProductNumberOverride':['FyProductNumberOverride', 'Fy Product Number Override','IsProductNumberOverride', 'ProductNumberOverride', 'Product Number Override'],
                            'FyProductNotes':['FyProductNotes', 'Fy Product Notes','ProductNotes','Product Notes','InternalProductNotes','Internal Product Notes','FYProductNotes','FY Product Notes'],

                            'WebsiteOnly':['WebsiteOnly', 'Website Only', 'Web Only'],
                            'ECATEligible':['ECATEligible', 'ECAT Eligible', 'ECAT Is Eligible', 'ECAT IsEligible'],
                            'HTMEEligible':['HTMEEligible', 'HTME Eligible', 'HTME Is Eligible', 'HTME IsEligible'],
                            'GSAEligible':['GSAEligible', 'GSA Eligible', 'GSA Is Eligible', 'GSA IsEligible'],
                            'VAEligible':['VAEligible', 'VA Eligible', 'VA Is Eligible', 'VA IsEligible'],

                            'ECATOnContract':['ECATOnContract', 'ECAT On Contract', 'ECAT On Contract'],
                            'HTMEOnContract':['HTMEOnContract', 'HTME On Contract', 'HTME On Contract'],
                            'GSAOnContract':['GSAOnContract', 'GSA On Contract', 'GSA On Contract'],
                            'VAOnContract':['VAOnContract', 'VA On Contract', 'VA On Contract'],

                            'IsDiscontinued':['IsDiscontinued','Discontinued','is discontinued','Is Discontinued'],

# GSA
                            'GSAApprovedListPrice':['GSAApprovedFYListPrice__value', 'GSAApprovedFYListPrice',
                                                    'GSAApprovedFYListPrice'],
                            'GSAApprovedPercent':['GSAChannelDiscount', 'GSADiscount', 'GSA Channel Discount',
                                                  'GSA Discount', 'gsa_channel_discount', 'GSA Approved Channel%','GSADiscountPercent'],
                            'GSABasePrice':['GSABasePrice', 'GSA Base Price'],
                            'GSASellPrice':['GSASellPrice', 'GSA Sell Price','current_gsa_price'],
                            'GSAContractModificationNumber':['GSAContractModificationNumber','GSAModificationNumber',
                                                             'GSA Contract Modification Number',
                                                             'GSAContractModification', 'GSA Contract Modification','GSA Modification Number'],
                            'GSAApprovedPriceDate':['GSAApprovedPriceDate', 'GSA Approved Price Date',
                                                    'GSA Approved Date'],
                            'GSA_Sin':['GSA_Sin', 'GSA_SIN', 'GSA Sin', 'GSA SIN', 'GSASin','GSA Sin #'],
# VA
                            'VAApprovedListPrice':['ChannelApprovedFYListPrice__value', 'ChannelApprovedFYListPrice',
                                                    'ApprovedFYListPrice'],
                            'VAApprovedPercent':['VAChannelDiscount', 'VADiscount', 'VA Channel Discount',
                                                  'VA Discount', 'va_channel_discount', 'VA Approved Channel%','VADiscountPercent'],
                            'VABasePrice':['VABasePrice', 'VA Base Price'],
                            'VASellPrice':['VASellPrice', 'VA Sell Price'],
                            'VAContractModificationNumber':['VAContractModificationNumber',
                                                             'VA Contract Modification Number',
                                                             'VAContractModification', 'VA Contract Modification','VAModificationNumber','VA Modification Number'],
                            'VAApprovedPriceDate':['VAApprovedPriceDate', 'VA Approved Price Date',
                                                    'VA Approved Date'],

                            'VA_Sin':['VA_Sin','VA_SIN','VA Sin','VA SIN','VASin','VA Sin #'],
# ECAT
                            'ECATApprovedListPrice':['ECATApprovedFYListPrice__value', 'ECATApprovedFYListPrice',
                                                   'ECATApprovedFYListPrice'],
                            'ECATApprovedPercent':['ECATChannelDiscount', 'ECATDiscount', 'ECAT Channel Discount',
                                                 'ECAT Discount', 'ecat_channel_discount', 'ECAT Approved Channel%'],
                            'ECATBasePrice':['ECATBasePrice', 'ECAT Base Price'],
                            'ECATSellPrice':['ECATSellPrice', 'ECAT Sell Price'],
                            'ECATContractModificationNumber':['ECATContractModificationNumber',
                                                            'ECAT Contract Modification Number',
                                                            'ECATContractModification', 'ECAT Contract Modification','ECATModificationNumber','ECAT Modification Number'],
                            'ECATApprovedPriceDate':['ECATApprovedPriceDate', 'ECAT Approved Price Date',
                                                   'ECAT Approved Date'],


                            'MfcDiscountPercent':['MfcDiscountPercent', 'Mfc Discount Percent', 'mfc_disc(%)',
                                                  'approved MFC per','VA_MFC_Discount','GSA_MFC_Discount'],
                            'MfcPrice':['MfcPrice', 'Approved MFC Price', 'Mfc Price'],

                            'DateCatalogReceived':['DateCatalogRecieved','DateCatelogReceived', 'DateCatalogReceived',
                                                   'Date Catalog Recieved', 'date_catalog_received','DateCatalogRcvd'],

                            'AllowPurchases':['AllowPurchases','Allow Purchases','Allow Purchases?'],
                            'ProductTaxClass':['ProductTaxClass','Product Tax Class','Tax Class'],
                            'Category':['CategoryRecommendation','Category','category'],

                            'ImageUrl':['ImageUrl', 'ProductImageUrl', 'Image Url', 'Product Image Url',
                                        'product_photo_url'],
                            'ImageName':['ImageName','ProductImageName','Image Name','Product Image Name','product_photo_Name'],
                            'ProductUrl':['URL','url','Product URL','Product Url','ProductUrl','product_url'],
                            'BCPriceUpdateToggle':['BCPriceUpdateToggle','BCPriceToggle','BC Price Update Toggle','BC Price Toggle'],
                            'BCDataUpdateToggle':['BCDataUpdateToggle','BCDataToggle','BC Data Update Toggle','BC Data Toggle']
                            }
        self.build_trans_dct()

    def build_trans_dct(self):
        self.translator_dict = {}
        for correct_head in self.dct_headers:
            for origin_head in self.dct_headers[correct_head]:
                self.translator_dict[origin_head.lower()] = correct_head


    def translate_headers(self,lst_in_heads):
        lst_out_heads = []
        for each_header in lst_in_heads:
            clean_head = str(each_header).strip()
            clean_head = clean_head.lower()

            if clean_head in self.translator_dict:
                if self.translator_dict[clean_head] not in lst_out_heads:
                    lst_out_heads.append(self.translator_dict[clean_head])
                else:
                    lst_out_heads.append(each_header)

            else:
                lst_out_heads.append(each_header)

        return lst_out_heads


## end ##