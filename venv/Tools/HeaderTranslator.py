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
                            'ManufacturerName':['ManufacturerName','Manufacturer Name','Manu Name','manufacturer_name','Manufacturer Standard Name'],
                            'SupplierName':['SupplierName','Supplier Name','supplier name','supplier_name'],

                            'VendorPartNumber':['VendorPartNumber','Vendor Part Number','primary_vendor_part_no','Thomas Item #'],
                            'CountryOfOrigin':['CountryOfOrigin','Country Of Origin','COO','Country','country_of_origin_code'],

                            'ManufacturerPartNumber':['GPS Manufacturer Part Number','ManufacturerPartNumber','manufacturer_part_number','manufacturer_part_no','Manufacturer Part Number','Manu Part #','MFR/Supplier Part#','Supplier Part#','SupplierPartNumber','manufacturer_part_no'],
                            'FyProductNumber':['FyProductNumber', 'Fy Product Number', 'Product Code/SKU'],
                            'FyPartNumber':['FyPartNumber', 'fy_part_number', 'fy_part_no'],

                            'ProductName':['ProductName','Product Name','product_name','primary_vendor_product_name'],
                            'ProductDescription':['ProductDescription','Product Description','product_description','ProductDesc','Product Desc','product_desc'],
                            'ShortDescription':['ShortDescription','Short Description','short_description','short_desc','ShortDesc','Short Desc'],
                            'LongDescription':['LongDescription','Long Description','long_description','long_desc','LongDesc','Long Desc'],
                            'ECommerceLongDescription':['ECommerceLongDescription','ECommerce Long Description','ecommerce_long_description'],

                            'Conv Factor/QTY UOM':['Conv Factor/QTY UOM', 'Conv Factor', 'Conversion Factor','UnitOfIssueCount', 'Unit Of Issue Count'],
                            'UnitOfIssue':['UnitOfIssue', 'UOI', 'uoi','unit_of_issue','unit_of_issue_qty'],
                            'UnitOfMeasure':['UnitOfMeasure','UOM','uom','uom_std'],
                            'LeadTimeDays':['LeadTimeDays','Lead Time Days','Lead Time','lead_time_days'],
                            'LeadTimeDaysExpedited':['LeadTimeDaysExpedited','Lead Time Days Expedited','Lead Time Expedited','expedited_lead_time_days','expedited_lead_time'],

                            'Vendor List Price':['Price','Vendor List Price','VendorListPrice','List Price','primary_vendor_list_price','vendor_list_price'],
                            'Discount':['Discount','VendorDiscount','Vendor Discount','Vendor Discount to FY','FyDiscountPercent','FyDiscByVendor'],
                            'Fy Cost':['Cost Price','Your Price','FyCost','Fy Cost','primary_vendor_fy_cost'],
                            'Fixed Shipping Cost':['Fixed Shipping Cost','Shipping Cost','FixedShippingCost','Estimated Shipping','EstimatedShipping'],
                            'Landed Cost':['FyLandedCost','LandedCost','Fy Landed Cost','Landed Cost'],
                            'LandedCostMarkupPercent_FYSell':['MarkUp','LandedCostMarkupPercent_FYSell', 'Markup', 'M/U Ron', 'MarkUp Ron', 'Mark Up Ron','M/U Ron','landed_cost_mark_up(%)'],
                            'Sell Price':['FySellPrice','FyPrice','BC Sell Price','Ecom Sell Price'],
                            'LandedCostMarkupPercent_FYList':['LandedCostMarkupPercent_FYList','M/U Linda','MarkUp Linda','Mark Up Linda','M/U Linda'],
                            'Retail Price':['Retail Price','FyListPrice','FyList'],

                            'MfcDiscountPercent':['MfcDiscountPercent','Mfc Discount Percent','mfc_disc(%)'],
                            'GSADiscount':['GSAChannelDiscount','GSADiscount','GSA Channel Discount','GSA Discount'],
                            'VADiscount':['VAChannelDiscount','VADiscount','VA Channel Discount','VA Discount'],

                            'DateCatalogRecieved':['DateCatalogRecieved','Date Catalog Recieved','date_catalog_received'],
                            'GSA_Sin':['GSA_Sin', 'GSA Sin', 'GSASin'],
                            'VA_Sin':['VA_Sin','VA Sin','VASin'],

                            'AllowPurchases':['AllowPurchases','Allow Purchases','Allow Purchases?'],
                            'ProductTaxClass':['ProductTaxClass','Product Tax Class','Tax Class'],
                            'Category':['CategoryRecommendation','Category'],

                            'ImageUrl':['ImageUrl','ProductImageUrl','Image Url','Product Image Url','product_photo_url'],
                            'Product URL':['URL','url','Product URL','product_url']}
        self.build_trans_dct()

    def build_trans_dct(self):
        self.translator_dict = {}
        for correct_head in self.dct_headers:
            for origin_head in self.dct_headers[correct_head]:
                self.translator_dict[origin_head] = correct_head


    def translate_headers(self,lst_in_heads):
        lst_out_heads = []
        for each_header in lst_in_heads:
            if each_header in self.translator_dict:
                if self.translator_dict[each_header] not in lst_out_heads:
                    lst_out_heads.append(self.translator_dict[each_header])
                else:
                    lst_out_heads.append(each_header)

            else:
                lst_out_heads.append(each_header)

        return lst_out_heads

