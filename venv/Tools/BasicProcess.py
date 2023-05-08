# CreatedBy: Emilia Crow
# CreateDate: 20210526
# Updated: 20220318
# CreateFor: Franklin Young International


import pandas
import numpy as np

from Tools.FY_DAL import DalObject
from Tools.Validation import Validator
from Tools.Extraction import Extractor
from Tools.Ingestion import IngestionObject
from Tools.HeaderTranslator import HeaderTranslator

from Tools.ProgressBar import YesNoDialog
from Tools.ProgressBar import ProgressBarWindow
from Tools.ProgressBar import JoinSelectionDialog


class BasicProcessObject:
    # this object requires a list called req_fields which is the set of headers that must be present in order to process the file/line
    # we should instead generate a list of fields that will be taken from the original df based on what is available
    # it would have sets of fall backs such as: productPriceId is missing therefore we need FYPartNo to be able to do the look up
    #
    # the process would basically check the first set of 'can't run without these' headers
    # followed by the sets of required, but fall-backable headers
    # then any that might be useful
    # together this will generate the take-set from the original
    # this will be the run set required for line processing
    def __init__(self, df_product, user, password, is_testing):
        self.name = 'Bob'
        self.message = 'No message'
        self.success = False
        self.is_viable = False
        self.set_new_order = False
        self.is_last = False
        self.np_nan = np.nan
        self.df_product = df_product
        self.obHeaderTranslator = HeaderTranslator()

        self.valid_product_units = ['BG','BO','BT','BX','CA','CT','DA','DR','DZ','FT','GA','GL','GM','GR','HR','IN','JR','KG','KT','LB','MO','MR','OZ','PC','PK','PL','PR','PT','RK','RL','RM','SE','SP','ST','TB','UN','VI','YD','YR']

        self.lst_product_headers, self.lst_untranslated_headers = self.obHeaderTranslator.translate_headers(list(self.df_product.columns))
        self.df_product.columns = self.lst_product_headers

        # remove duplicated columns by headers
        self.df_product = self.df_product.loc[:, ~self.df_product.columns.duplicated()]

        self.out_column_headers = self.df_product.columns
        self.return_df_product = pandas.DataFrame(columns=self.df_product.columns)

        self.header_viability()
        if self.is_viable:
            self.obDal = DalObject(user, password)
            alive = self.obDal.ping_it(is_testing)
            if alive == 'Ping':
                self.object_setup(is_testing)
            else:
                self.message = alive
                self.is_viable = False


    def header_viability(self):
        # if there are required headers we check if they're all there
        if len(self.lst_untranslated_headers) > 0:
            self.is_viable = False
        else:
            product_headers = set(self.lst_product_headers)
            if len(self.req_fields) > 0:
                required_headers = set(self.req_fields)
                self.is_viable = required_headers.issubset(product_headers)
                # if it passes and there are support headers we check them
                if (len(self.sup_fields) > 0) and self.is_viable:
                    support_headers = set(self.sup_fields)
                    if len(product_headers.intersection(support_headers)) == 0:
                        self.is_viable = False

            # there aren't required headers, but there are support headers
            elif len(self.sup_fields) > 0:
                support_headers = set(self.sup_fields)
                if len(product_headers.intersection(support_headers)) > 0:
                    self.is_viable = True
            else:
                self.is_viable = True

    def get_missing_heads(self):
        product_headers = set(self.lst_product_headers)
        required_headers = set(self.req_fields)
        return list(required_headers.difference(product_headers))

    def vendor_name_selection(self):
        lst_vendor_names = self.df_vendor_translator['VendorName'].tolist()
        self.obVendorTickBox = JoinSelectionDialog(lst_vendor_names, 'Please select 1 Vendor.')
        self.obVendorTickBox.exec()
        # split on column or column
        vendor_name_list = self.obVendorTickBox.get_selected_items()
        vendor_name = vendor_name_list[0]
        return vendor_name

    def object_setup(self,is_testing):
        self.obValidator = Validator()
        self.obExtractor = Extractor()
        self.obYNBox = YesNoDialog()

        self.obIngester = IngestionObject(self.obDal)
        self.df_country_translator = self.obIngester.get_country_lookup()
        self.df_category_names = self.obIngester.get_category_names()
        self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
        self.df_vendor_translator = self.obIngester.get_vendor_lookup()


    def set_progress_bar(self, count_of_steps, name):
        self.obProgressBarWindow = ProgressBarWindow(name)
        self.obProgressBarWindow.show()
        self.obProgressBarWindow.set_anew(count_of_steps)


    def batch_process_vendor(self):
        print('Vendor matching')
        df_attribute = self.df_product[['VendorName']]
        df_attribute = df_attribute.drop_duplicates(subset=['VendorName'])
        lst_ids = []
        for colName, row in df_attribute.iterrows():
            vendor_name = row['VendorName'].upper()
            if vendor_name in self.df_vendor_translator['VendorCode'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
            elif vendor_name in self.df_vendor_translator['VendorName'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
            else:
                vendor_name_list = self.df_vendor_translator["VendorName"].tolist()
                vendor_name_list = list(dict.fromkeys(vendor_name_list))

                new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name,lst_vendor_names=vendor_name_list)

            lst_ids.append(new_vendor_id)

        df_attribute['VendorId'] = lst_ids

        self.df_product = self.df_product.merge(df_attribute, how='left', on=['VendorName'])


    def batch_process_manufacturer(self):
        print('Manufacturer matching')

        blocked_manufacturers = ['ABCAM','ANDE CORPORATION','ANGENEX','ANSH LABS','AOBIOUS, INC','APEX ADVERTISING'
        ,'APOGEE DESIGNS LIMITED','ART SUPPLY WAREHOUSE INC','ATTOGENE','B & C PRECISION MFR CO','BAY STATE CRUCIBLE CO'
        ,'BBC BIOCHEMICAL','BERK INTERNATIONAL LLC','BIOPULSE PRODUCTS CO INC','BIOSERO INC','BIOTAGE'
        ,'BIOTOOLS PTY LTD','BLACK MACHINE COMPANY','BOOTIE BUTLER','BOSCOM','BRANDT BOX & PAPER','BRASEL PRODUCTS INC'
        ,'BRAUN BRUSH COMPANY','BREEZE INDUSTRIAL PRODUCTS','BRIDGEPATH SCIENTIFIC','BRIM TECHNOLOGIES'
        ,'BUCKEYE CORRUGATED INC','CAMBRIDGE ENVIRONMENTAL','CCP INDUSTRIES','CELLEX INC','CLEANROOM SYNERGY LLC'
        ,'CLEATECH','COMPASS INDUSTIRES','CONSOLIDATED PLASTICS','CONTOUR DESIGN INC','COPYPRO','CORTEX PRODUCTS INC'
        ,'CREATIVE PEG WORKS','CREATIVESTAR SOLUTIONS','CURATIVE DX','CUSTOM SOLUTIONS GROUP LLC','DECAGON DEVICES INC'
        ,'DECKER TAPE PRODUCTS','DEL VEL CHEMICAL','DSMZ','DSX','DUN-RITE MACHINING','DURABLE CORPORATION'
        ,'EASTERN DISTRIBUTING','EDMUND OPTICS','ELEKTRO PHYSIK','ENDOCH','ENGELHARD','ENVIRONCYCLE/SURFCHEM'
        ,'ERASER COMPANY','ESCO TECHNOLOGIES','ESTIGEN OU','EXOLOGIX','EXPEDEON','FIBERWARE INC','FIRERESQ INC'
        ,'FORTIUSBIO','FOUR SEASONS PPE','GEERPRES','GLOBEPHARMA INC','GNOMEGEN LLC','GRIFFIN CARE','HALL PRODUCTIONS'
        ,'HARDEDGE TOOL','HARTCO INC','HAZARD TECHNOLOGY','HOEFER','HR PHARMECEUTICALS INC','I TEAM NORTH AMERICA'
        ,'INDUSTRIAL CHEM SOLUTIONS','INTERNATIONAL LAB SUPPLY','INTRA STORAGE SYSTEMS INC','JDM COLLABORATIONS'
        ,'JNJ INDUSTRIES INC','KAPPLER','KBIOSYSTEMS LIMITED','KIPP & ZONEN','KM ACT CORPORATION'
        ,'KM HEALTHCARE CORPORATION','LGL CONSULTION INC','LIFOAM INDUSTRIES LLC','LIGHTING SPECIALTIES CO','LITHCO INC'
        ,'LUMINOSTICS INC','LUX PRODUCTS','MAC-MOD ANALYTICAL INC','MAR COR PURIFICATION','MARKET FORGE INDUSTRIES'
        ,'MARTOR USA','MASTECH GROUP LLC','MEDICAL ARTS PRESS','MEDPAPER','MERCEDES MEDICAL','MERITECH INC'
        ,'MICROBIO PRODUCTS','MILLER-STEPHENSON CHEMICAL CO INC','MOBILE JANITORIAL SUPPLY','MODULUS DATA SYSTEMS'
        ,'MONERA TECHNOLOGIES CORPORATION','MOTO SOLUTIONS','O2SI','PACIFIC HANDY CUTTER INC','PACKAGE ALL CORP'
        ,'PAW BIOSCIENCE PRODUCTS INC','PENNSYLVANIA GLASS PRODUCTS','PHENOMENEX','PHENOVA','PLASTIC PROCESS EQUIPMENT'
        ,'PREFERRED MEDICAL','PREMIER MAGNESIA','PRO-TAPES & SPECIALTIES','QUANTUM WEST COAST DISTRIBUTION','QUILL'
        ,'R & B WHOLESALE DISTRIBUTORS, INC','R.C. SMITH','R3 PAPERCRAFT LA','RADNOTI GLASS TECHNOLOGY','RENISHAW'
        ,'RIMCO PLASTICS CORPORATION','RIVERSTONE RESOURCES','ROMER','ROSTRA VERNATHERM','RTI ELECTRONICS, INC'
        ,'SAF-T-GARD INTERNATIONAL INC','SANDLER BROS','SANOTECH 360 LLC','SEOUL GUIDE KOREA CO LTD'
        ,'SHENZHEN JDS MEDICAL SUPPLY CHAIN','SHIELD MARK INC','SIMCO-ION','SMALL PRECISION TOOLS','SMART TUBE INC'
        ,'SMITH ECOLOGICAL SYSTEMS CO','SOCAL BATTERIES','SPECTRUM SOLUTIONS','SPRAYWAY INC','STAND UP SOLUTIONS'
        ,'STERIMARK INC','STERI-TEK','STEWART R BROWNE MFG CO INC','STIMPSON','SWAGELOK NORTHERN CALIFORNIA'
        ,'SWENCO LTD','SYSTECH ILLINOIS INSTRUMENTS','TARSONS','TDI2 CUSTOM PACKAGING INC','TECHNI-TOOL INC'
        ,'TEXTILE-BASED DELIVERYDBA NUFABRX','THE BAKER COMPANY','THE VERNON COMPANY','TPT GLOBAL TECH INC','TSI INC'
        ,'UFP TECHNOLOGIES INC','UNITED PAN AMERICAN INC DBA WILCO ENTERP','UV PROCESS SUPPLY INC'
        ,'VERITIV OPERATING COMPANY','VICAM','VICTORY FOAM','WEBSTAURANT STORE','WENZHOU AISHI OPTICS INDUSTRY CO LTD'
        ,'WHITE MOUNTAIN IMAGING','WIELAND METAL SERVICES FOILS','WILBUR TECHNICAL SERVICES','WORKING CONCEPTS'
        ,'YOUNG MICROBRUSH','Y-PERS','ZHEJIANG RUNLAB TECHNOLOGY','DELL','MACCO ORGANIQUES','NANOENTEK','AVALON LABELS'
        ,'CALIBREX TECHNOLOGIES','DANYANG YIFAN PACKAGING','ENVIROSTAR','FIVE-TEN TECHNOLOGY','GLOBAL FOOTPRINTS'
        ,'HISCO','INOCTA SOLUTIONS','KIRSCH LIGHTING','LADDAWN','LOTUS CONNECT','MALLORY SAFETY & SUPPLY'
        ,'MARK ONE MANUFACTURING','MAXONS TECHNOLOGIES','NON CATALOG','OFFICE DEPOT','OSS TECHNOLOGY'
        ,'POSTAL PRODUCTS UNLIMITED','PRICELESS RESOURCE INC','QAQC LAB','REAGENT GENIE','RENCO CORPORATION'
        ,'REYTEK EQUIPMENT','SAFETEC OF AMERICA','SHANGHAI GENE ERA BIO-SCIENCE','SMILE STREAM','HOME DEPOT'
        ,'TRUCKEE APPLIED GENOMICS','XIANTAO SANDA INDUSTRIAL','ZHEJIANG KANGMIN MEDICAL','ZHEJIANG PEKY BIOTECH'
        ,'3D SYSTEMS','A.J. KIRBY','ACCUSPLIT','ADVENTURE PRODUCTS','ADVENTUREKEEN','AGELA TECHNOLOGIES'
        ,'AGI THE GLASS GALLERY','ALAN FORAL','ALLEVI INC','AMAZON','AMD MEDICOM','ANDREWS UNIVERSITY'
        ,'ANTHONY ZVIRBLIS','APPLIED PRACTICE','AQUANEERING','AQUARIUM SYSTEMS','ASHEVILLE-SCHOONMAKER MICA'
        ,'ATLANTIC METAL INDUSTRIES','ATLANTIC SCHMELZTIEGEL','ATLAS B E','ATL-EAST TAG & LABEL','B & B REFRACTORIES'
        ,'BAKEMARK','BAR CHARTS','BAXTER SALES & DISTRIBUTION','BDS ROCKS','BELT INC','BENNETT M. HARRIS','BFG SUPPLY'
        ,'BIG OAK DRIVING RANGE','BIO ARRA','BIOLOGICAL RESOURCE','BIOMEDIA','BIOSIG INSTRUMENTS','BJ SCIENTIFIC'
        ,'BLACKWELL PUBLISHING','BLICK ART MATERIALS','BLOOD GLOW','BMX KIT','BOGERT & HOPPER','BORDER STATES ELECTRIC'
        ,'BRITTEN BANNER','BROILKING','BROWN-WOOD PRODUCTS','BUFFALO HOTEL SUPPLY','BURDICK & JACKSON'
        ,'BUSHNELL CORPORATION','C.H.SPENCER','C.R. BARD','CAFFCO PRODUCTS','CALIFORNIA PACIFIC LAB'
        ,'CAPEWELL AERIAL SYSTEMS','CASCADES CONTAINERBOARD PACKAGING','CATALYST LEARNING','CCL LABEL SIOUX FALLS'
        ,'CENTER ENTERPRISES','CERCO','CHARTPAK','CHICAGO MAILING TUBE COMPANY','CLIFFORD W. ESTES','CLOUD CHART'
        ,'COASTAL SILKWORMS','COATS & CLARK','COMMUNITY LEARNING','COMPLEMAR PRINT','CONCOA CORPORATION'
        ,'CONTAINER MANAGEMENT SERVICES','CONTIMA','CONTROL SENECA','COOK IRON','COPERNICUS TOYS','CORDS CANADA'
        ,'COVINGTON ENGINEERING','CRAFTSTONES','CRAMER SOFTWARE','CRC PRESS','CREATIVE TEACHING PRESS','CROSMAN SEED'
        ,'CUMINGS','CYGNUS','CYRGUS','CYRS BIOLOGY','D&L COMPANY','DARICE','DARWIN CHAMBERS','DAS WOODWORKING'
        ,'DASCO PRO','DEEPWATER IODIDES','DENCO EQUIPMENT','DENNIS J MUCENSKI','DFX INTERACTIVE','DIAMOND TOOL & DIE'
        ,'DIDAX EDUCATIONAL RESOURCES','DIGI-TRAX','DISTRIBUTION INTERNATIONAL','DISTRIBUTION RESULTS'
        ,'DIVAL SAFETY & SUPPLIES','DIVNICK INTERNATIONAL','DMF BAIT','DOJES FORENSIC SUPPLIES','DONEGAN OPTICAL'
        ,'DOUGLAS FROST','DOUGLAS NET','DOVER PUBLICATIONS','DR INSTRUMENTS','DR. GARY OLMSTEAD','DRI MARK PRODUCTS'
        ,'DSS DISTRIBUTING','DUANE TYNSKY','DURACOTE','DYCEM','E W PIKE','E. K. INDUSTRIES'
        ,'EARTH SCIENCE INTERNATIONAL','EAST PATTERN & MODEL','EATON OFFICE SUPPLY','EBONEX','ECHO'
        ,'ECONOMY PRODUCT AND SOLUTIONS','ELECTROCORR','ELENCO ELECTRONICS','ELIZABETH WALDORF','ELMER SCHULTZ SERVICES'
        ,'EMERALD PRINT MANAGEMENT','EMPIRE/EMCO','ENDICOTT-SEYMOUR','ENERGIZER BATTERY','ENERGY TECHNOLOGY'
        ,'ENGINEERED SYSTEMS & DESIGNS','EP SALES','EPSCO','ERIC BANDURSKI','ESM INC ENTERPRISES','EVEREST'
        ,'EVIDENT CRIME SCENE PRODUCTS','WHITE KNIGHT','IMMUNOSTICS','IN-SITU','MARCOM GROUP','YOUNG IN FRONTIER'
        ,'HTL-STREFA INC.','ILIUM TECHNOLOGY INC','HALL SAFETY APPAREL INC','CODEX DNA INC','IDEXX DISTRIBUTION CORP.'
        ,'WATERBORNE, INC.','PROTEXER, INC.','INVITRIA','HEMOSURE INC.','PHOTRON PTY LTD.','GTG PACKAGING'
        ,'NEW LISKEARD SHEET METAL WORKS','METSO MINERALS INDUSTRIES INC.','TYCO FIRE PRODUCTS'
        ,'UNISOURCE MANUFACTURING INC','PENETONE CORPORATION','SAFTCART','MARKEL INDUSTRIES INC.','SAVAGE UNIVERSAL INC'
        ,'LIPPINCOTT WILLIAMS & WILKINS','RAVEN MAPS & IMAGES','POWER TOOLS & ABRASIVES','TAPECON INC'
        ,'GALACTIC BALLOONS','JEFF RAMSEY','SARUT GROUP','KNOWLEDGE UNLIMITED INC.','RALPH FUDGE'
        ,'T. GRIFFITH JONES, III','GEOPACKS','OXFORD UNIVERSITY PRESS, INC.','TRU-SQUARE METAL PRODUCTS'
        ,'MILLER ENTERPRISES OF OHIO LLC','K D BIOGRAPHICS SE','OMNI RESOURCES INC','FAIRGATE RULE CO., INC.'
        ,'PM RESEARCH INC.','INSURANCE INSTITUTE FOR HIGHWAY SAFETY','TRANSILWRAP COMPANY, INC.'
        ,'MATHESON TRI-GAS, INC.','SOCIAL STUDIES SCHOOL SERVICE','PROMARK INTERNATIONAL, INC.'
        ,'MELLEN MARKETING ASSOCIATES','SMITHERS OASIS CO','PECA PRODUCTS INC.','HUNTAR CO INC','PRATT-READ TOOLS SE'
        ,'LANSKY SHARPENERS','PLANET EARTH GIFTS','MARATHON MANAGEMENT','GEOGRAPHIC RESOURCE SOLUTIONS'
        ,'WILDLIFE SUPPLY CO.','SUN COMPANY, INC.','THOUGHT TECHNOLOGY, LTD','ZANDUR CORK PRODUCTS'
        ,'HAWKEYE STEEL PRODUCTS INC','SIRCHIE','PETTINGER SHEET & METAL','VIKING MANUFACTURING COMPANY'
        ,'HEALTH SMART SE','WALTER PRODUCTS','TECH-OPTICS INTERNATIONAL','FOSTER MANUFACTURING COMPANY'
        ,'LYMAN PRODUCTS CORPORATION','INDEPENDENT PUBLISHERS GROUP','MCDONALD & WOODWARD PUBLISHING','MAD RIVER PRESS'
        ,'MORTON PUBLISHING CO','PEOPLES PUBLISHING GROUP','FELINE PRESS','JOHN T. SULLIVAN','STEPHEN GOULD CORPORATION'
        ,'HARRIS E. FERGUSON','LESLIE HEINZL','JACK GRANT','SGE','THE BUG HOUSE','ISEE SYSTEMS, INC.'
        ,'SKYWILL FAR EAST INDUSTRIES','PBS DISTRIBUTION, LLC','PETRO PACKAGING CO. INC.','JOEL C. PALMER'
        ,'HOLIDAY HOUSE, INC.','DYNO CORPORATION','J&B IMPORTERS','OGS TECHNOLOGIES, INC.','SOUTHWIRE COMPANY, LLC.'
        ,'GAME PARTS','JAMES B LOGGANS','UNIVERSAL SPECIALTIES SE','PROFILE PLASTICS INC','POLMAN MINERALS'
        ,'FIRST TEXAS PRODUCTS, LLC','SMITH STUDIOS','FITZHENRY & WHITESIDE LIMITED','PERMACHARTS','JOHN P. MARECHAL'
        ,'ROCK DETECTIVE','IMPACT PRODUCTS','HENRIETTA COUNTRY MAX','ISHWARA INTERNATIONAL','KIN CORE INC.'
        ,'L&R MANUFACTURING COMPANY','SIMCONA ELECTRONICS CORP.','IZORLINE INTERNATIONAL, INC','PENGUIN GROUP INC.'
        ,'HUFFYS AIRPORT WINDSOCKS','FINLEY HOLIDAY FILM CORP.','KAPPA MAP GROUP LLC','MTI CORP CP','PRENTICE HALL SE'
        ,'FACTS ON FILE INC','OKLAHOMA SOUND','ISIMET SE','GOW-MAC INSTRUMENT CO','TEL-ATOMIC INC','PALO ENTERPRISES'
        ,'GC/WALDOM ELECTRONICS SE','MOLECULAR MODEL ENTERPRISES','SPIRING ENTERPRISES LTD','RIEGLE PRESS INC'
        ,'PLATT BROTHERS & CO','TOLCO CORP SE','THE BRUCE WOOD COLLECTION','INFINITE TEACH, NV INC','J & B PRODUCTS'
        ,'ICC THE COMPLIANCE CENTER INC.','SENSGARD LLC','SPYGLASS PUBLICATIONS','WADDINGTON NORTH AMERICA'
        ,'GARDENERS SUPPLY COMPANY','F.W. SCHUMACHER CO., INC.','ONTARIO PLASTICS INC','F&H RIBBON COMPANY'
        ,'PHYTOTECH LABS','MINERAL EDUCATION COALITION','NICHOLS NET & TWINE','THE GLOBAL EDUCATION PROJECT'
        ,'WESTERN HILLS ROCK & GEMS, INC','MICHAEL D. MIGLIETTA','OCEAN RESOURCES, INC.','THE BEST CONNECTION INC'
        ,'WABASH INSTRUMENTS CORP SE','JEFFREY M BUBEL','ORNAMENTAL PLANTS AND TREES','LEVITON MFG. DEPT.'
        ,'SOODHALTER PLASTICS INC.','GULF OF MAINE, INC.','SILVER CIRCLE PRODUCTS','GOURMET MUSHROOMS INC.'
        ,'MOYERS CHICKS INC','OSI MARINE LAB','ASAHI/AMERICA','AW-LAKE COMPANY','BERNS CORP','BHS MARKETING','BIOLOG'
        ,'CALCO LTD','DENLINE UNIFORMS','DIVERSEY','DUTCH VALLEY FOOD DISTRIBUTORS','ENTEGRIS','ERIC MILLER'
        ,'FALCONE ELECTRICAL SUPPLY','FIBERWEB','FIELD TESTED SYSTEMS','FILMS FOR THE HUMANITIES & SCI'
        ,'FIREKING INTERNATIONAL','FLAMBEAU COMM MARKET GROUP','FLIPPERGUIDES','FLORIDA AQUA FARMS'
        ,'FLORIDA MARINE RESEARCH','FOSECO','FOSSILCRAFT ENTERPRISES','FREDRIC ELLIOTT COMPANY'
        ,'Fresenius Kabi','FULLER G C MFG','FULMIN INTERNATIONAL','FUN EXPRESS','GENERAL ELECTRIC'
        ,'GOLDCREST ELECTRONICS','GOPHER SPORT','GOSSMANN PROJECTS','GQF MANUFACTURING','GRAN ADELL MFG'
        ,'GRAYBAR ELECTRIC','GREGSON INDUSTRIAL SUPPLY','GRIFFIN GREENHOUSE & NURSERY','H.A. STILES COMPANY'
        ,'HACHETTE BOOK GROUP USA','HAMMOND W.A. DRIERITE','HANDY ART','HARDLEY DANGEROUS ILLUSIONS'
        ,'HAWKINS CHEMICAL CO','HEDWIN DIVISION OF ZACROS AMERICA','HELIOCENTRIS','HERO ARTS RUBBER STAMPS'
        ,'HOUSE OF CANS','HSP NATURE TOYS','HUB CITY INDUSTRIES','HUGH COURTRIGHT & CO','IMPERIAL DADE'
        ,'INDUCTOTHERM CORP','INDUSTRIAL CONTAINER & SUPPLY','INDUSTRIAL CONTAINER SERVICE','INKSMITH LIMITED'
        ,'INTERNATIONAL VIDEO & ELECTRONICS','ITOYA OF AMERICA','JW CLARK','JANEVAL TOYS','JARDEN HOME BRANDS'
        ,'JAY C WILEY','JELLIFF CORP','JOHN HENRY COMPANY','JOLLY LOGIC','JT & A','JUNKIN SAFETY APPLIANCE'
        ,'KEHR-BUFFALO WIRE FRAME','KISMET RUBBER PRODUCTS CORP','KNOW YOUR IONS','KR ANDERSON CO TE'
        ,'LAMSON & GOODNOW MFG CO','LD DIDACTIC','LEARN ENGINES','LEE COUNTY REPROGRAPHICS'
        ,'LEONARD LIGHT INDUSTRIES (PTY)','LIBERTY MOUNTAIN SPORTS','LIOFILCHEM','LIQUID CRYSTAL RESOURCES'
        ,'LITTLE FOLKS VISUALS','L-K INDUSTRIES','LKG INDUSTRIES','LUSTER LEAF PRODUCTS','MACO BAG CORPORATION'
        ,'MAJOR SCIENCE BE','MALCOLM G STEVENS','MARK D PALATAS','Marlin Steel Wire Products','MASTER MAGNETICS'
        ,'MASTER REPLICAS GROUP','MAYER HARDWARE','MEDAX INTERNATIONAL','MedCision','MEDICAL INDICATORS','Medicom'
        ,'MEDIVIBE TECHNOLOGY','MEIJI TECHNO AMERICA','METSO OUTOTEC USA','MIDWEST GEOSCIENCES','MINCO','MINERALAB'
        ,'MJM INCENTIVES','MONTCLARE SCIENTIFIC GLASS','MONTROY SUPPLY CO','MOODY TOOLS','MOORECO','MRS STEWARTS BLUING'
        ,'MSI FORENSICS','MULTISORB TECHNOLOGIES','MUSCO SPORTS LIGHTING','MYERS-STEVENS GROUP','MYRON L COMPANY'
        ,'NATIONAL GEOGRAPHIC MAPS','NEOTEK','NILES BIOLOGICAL','NOIR MEDICAL','NORMAN LAMPS','NORTH AMERICAN GEAR'
        ,'NORTH AMERICAN PAPER CO','NUNC INC','NURSE ASSIST','NUTHEME SAFETY SIGN','OAKFIELD APPARATUS CO'
        ,'OCEAN OPTICS','OFFICE STAR','OFM INC','ONPOINT LASERS','ORMANTINE USA LTD','OTHER WORLDS EDUCATIONAL ENT'
        ,'OWI INCORPORATED','OWL BRAND SUPPLY COMPANY','PACIFIC TRANSDUCER CORP','PAN TERRA','PARADIGM EDUCATION CORP'
        ,'PARAGON MANUFACTURING','PARKED WHOLESALE','PARKER PALEONTOLOGICAL ENT','PATHCO','PEARSON EDUCATION'
        ,'PERFECTO MANUFACTURING','PETERSON FILTERS CORPORATION','PF TECHNOLOGIES','PHILIP BRYAN WOODARD'
        ,'PHILPAC CORPORATION','PICOSOLUTIONS GROUP','PIEDMONT PLASTICS','PIONEER NATIONAL LATEX'
        ,'POLYFORM PRODUCTS COMPANY','PORTWEST','PREFERRED BEEF','PREMIUMS FOR INDUSTRIES','PROGRESSIVE PACKAGING'
        ,'PROTEIN SIMPLE','PYRADIA','RAINWISE','RDA CONTAINER CORPORATION','REBECCAS','RED BIRDRVICE'
        ,'RHYTHM BAND INSTRUMENTS','ROBERT BOSCH TOOL CORPORATION','ROBERT CRESPO','ROCKFORD MFG CO','ROLL PRODUCTSINC'
        ,'RPM MIDWEST SALES','RUNCO OFFICE SUPPLY & EQUIP CO','S E INTERNATIONAL','S R P INC','SAN FRANCISCO BAY BRAND'
        ,'SANDTASTIK PRODUCTS','SANFORD CORP','SCC PUMPS','SCHOOL SPECIALTY','SCOTT RESOURCES 2','SEASTAR CHEMICALS'
        ,'SEEDBURO EQUIPMENT CO','SEEDS SOFTWARE','SEEDWAY','SEMPCO','SERFILCO','SHANKS EXTRACTS','SIENCO INC'
        ,'SIERRA CHEMICAL COMPANY','SILVER LAKE RESEARCH CORP','SIMONDS SAW','SIPEL ELECTRONIC SA','SOLO HORTON BRUSHES'
        ,'SPI/SEMICON','SQUARE ONE PUBLISHERS','STEEL & O\'BRIEN MANUFACTURING','SUNDANCE/NEWBRIDGE'
        ,'SUNDOWN INDUSTRIES','SUPERIORWING MACHINE & SUPPLY','SURGICAL SPEC PUERTO RICO','SUUNTO USA','TAMIYA AMERICA'
        ,'TAPECASE LTD TE','TAYLOR ENVIRONMENTAL INSTR','TEACHERS DISCOVERY','Tedia','TESSELLATIONS','THAMES & KOSMOS'
        ,'THE APPRENTICE CORPORATION','THE DOUGLAS STEWART CO','THE LYNN PEAVEY COMPANY','THE SCIENCE SOURCE'
        ,'TICKLEME PLANT COMPANY','TLF GRAPHICS','TOOL SPECIALTY CO','TOYSMITH','TREASURE CREEK MINERALS'
        ,'US GEOLOGICAL SURVEY','US SAFETY','UCHIDA YOKO GLOBAL CO','UNITED STATES PLASTICS CORP'
        ,'UNIVERSAL LEARNING SOLUTIONS','UNIVERSITY OF CALIFORNIA','US COTTON','VALAX MANUFACTURING CA'
        ,'VANTON PUMP & EQUIPMENT CORP','VERONICA MATTHEWS MINERALS','VICTORY VR INC','W B MASON CO'
        ,'WAGNER PETROGRAPHIC','WAREHOUSE 115','WATERS CHEMISTRY','WATERWISE','WCM MINERALS','WELOC USA'
        ,'WHAT IF SCIENTIFIC','WIHA TOOLS','WILDEN PUMP AND ENGINEERING','WIRE CLOTH MANUFACTURERS','WOLF MACHINE'
        ,'Worldwide Products','Xenopus-1','YEANY EDUCATIONAL PRODUCTS','Z-MAR TECHNOLOGY']

        df_attribute = self.df_product[['ManufacturerName']]
        df_attribute = df_attribute.drop_duplicates(subset=['ManufacturerName'])
        lst_ids = []
        lst_names = []
        lst_is_blocked = []

        for colName, row in df_attribute.iterrows():
            manufacturer_name = row['ManufacturerName'].upper()
            manufacturer_name = self.obValidator.clean_manufacturer_name(manufacturer_name,False)
            while '  ' in manufacturer_name:
                manufacturer_name = manufacturer_name.replace('  ',' ')

            new_manufacturer_name = manufacturer_name
            if manufacturer_name == '':
                new_manufacturer_id = -1
            elif manufacturer_name.lower() in self.df_manufacturer_translator['SupplierName'].values:
                new_manufacturer_id = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == manufacturer_name.lower()),'ManufacturerId'].values[0]
                new_manufacturer_name = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == manufacturer_name.lower()),'ManufacturerName'].values[0]
            elif manufacturer_name in self.df_manufacturer_translator['ManufacturerName'].values:
                new_manufacturer_id = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['ManufacturerName'] == manufacturer_name),'ManufacturerId'].values[0]
            else:
                manufacturer_name_list = self.df_manufacturer_translator["ManufacturerName"].tolist()
                manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))
                new_manufacturer_id = self.obIngester.manual_ingest_manufacturer(atmp_sup=manufacturer_name.lower(), atmp_man=manufacturer_name, lst_manufacturer_names=manufacturer_name_list)

            if new_manufacturer_name in blocked_manufacturers:
                b_is_blocked = 1
            else:
                b_is_blocked = 0

            lst_ids.append(new_manufacturer_id)
            lst_names.append(new_manufacturer_name)
            lst_is_blocked.append(b_is_blocked)

        df_attribute['ManufacturerId'] = lst_ids
        df_attribute['UpdateManufacturerName'] = lst_names
        df_attribute['BlockedManufacturer'] = lst_is_blocked

        self.df_product = self.df_product.merge(df_attribute, how='left', on=['ManufacturerName'])

        self.df_product['ManufacturerName'] = self.df_product[['UpdateManufacturerName']]


    def define_new(self, b_match_vendor = False):
        clear_headers = ['UpdateManufacturerName', 'ManufacturerId', 'ProductId','ProductPriceId','BaseProductPriceId','db_ProductNumberOverride','db_IsDiscontinued','db_FyIsDiscontinued',
                         'ECATProductPriceId', 'HTMEProductPriceId','GSAProductPriceId','VAProductPriceId','db_FyProductNotes',
                         'db_VAProductNotes','db_GSAProductNotes','db_HTMEProductNotes','db_ECATProductNotes','TakePriority','BlockedManufacturer']
        for each_header in clear_headers:
            if each_header in self.df_product.columns:
                self.df_product = self.df_product.drop(columns=[each_header])

        if b_match_vendor:
            self.batch_process_vendor()

        self.batch_process_manufacturer()

        print('Agni Kai')
        self.obProgressBarWindow.update_bar(1)
        """
          M C P V : types of product ids
        1 X X X X : These are called ready (unless they need pricing in which case Update-BasePrice)
        2 X X X O : these are from a different vendor, Update-productprice
        3 X X O X : these are a different size, Update-productprice
        4 X X O O : these are likely missing vendor info, Update-productprice
        5 X O     : These are likely overrides
        6 O X     : These are manufacturer name updates
        7 O O     : new
        """
        # get product look up
        self.df_product_agni_kai_lookup = self.obDal.get_product_action_review_lookup()
        # we create a df for the product notes alone so they can be added at the end.
        self.df_product_notes = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['db_FyProductNotes']!= '')]
        drop_notes = ['ProductId', 'ManufacturerName', 'ManufacturerPartNumber', 'FyCatalogNumber','FyProductNumber',
                      'VendorName','VendorPartNumber','BaseProductPriceId','db_IsDiscontinued','db_FyIsDiscontinued',
                      'ECATProductPriceId', 'HTMEProductPriceId','GSAProductPriceId','VAProductPriceId']
        self.df_product_notes = self.df_product_notes.drop(columns=drop_notes)
        self.df_product_agni_kai_lookup = self.df_product_agni_kai_lookup.drop(columns=['db_ProductNumberOverride','db_FyProductNotes','db_VAProductNotes','db_GSAProductNotes','db_HTMEProductNotes','db_ECATProductNotes'])

        # set up the different match types
        # all products in DB with pricing
        self.df_full_product_lookup = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['BaseProductPriceId'] != 'Load Pricing')].copy()
        self.df_full_product_lookup['Filter'] = 'Ready'
        self.df_product_agni_kai_lookup = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['BaseProductPriceId'] == 'Load Pricing')]

        # all products without pricing but with a vendor
        self.df_product_price_lookup = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['ProductPriceId'] != 'Load Product Pricing')].copy()
        self.df_product_price_lookup['Filter'] = 'Base Pricing'
        self.df_product_price_lookup = self.df_product_price_lookup.drop(columns = ['BaseProductPriceId','db_IsDiscontinued','db_FyIsDiscontinued','ECATProductPriceId', 'HTMEProductPriceId','GSAProductPriceId','VAProductPriceId'])

        # all products without vendor
        self.df_product_minumum_lookup = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['ProductPriceId'] == 'Load Product Pricing')].copy()
        self.df_product_minumum_lookup['Filter'] = 'Partial'
        self.df_product_minumum_lookup = self.df_product_minumum_lookup.drop(columns = ['ProductPriceId'])


        # match all values, these are ready files
        print('Round 1')
        self.obProgressBarWindow.update_bar(2)
        merge_columns = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber', 'FyProductNumber', 'VendorName','VendorPartNumber']
        self.df_product, self.df_full_matched_product, self.df_full_product_lookup = self.merge_and_split(self.df_product, self.df_full_product_lookup, merge_columns)

        # match all values, these are Base Price files
        print('Round 2')
        self.obProgressBarWindow.update_bar(3)
        self.df_product, self.df_pricing_matched_product, self.df_product_price_lookup = self.merge_and_split(self.df_product, self.df_product_price_lookup, merge_columns)
        self.df_price_agnostic_product_lookup = pandas.concat([self.df_full_product_lookup, self.df_product_price_lookup, self.df_product_minumum_lookup], ignore_index=True)
        self.df_price_agnostic_product_lookup.drop_duplicates(['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber'] , ignore_index = True, inplace = True)

        # round 3
        print('Round 3')
        self.obProgressBarWindow.update_bar(4)
        self.df_price_agnostic_product_lookup['Filter'] = 'Partial'
        self.df_product = self.df_product.merge(self.df_price_agnostic_product_lookup, how='left',on=['FyCatalogNumber','ManufacturerPartNumber'])
        self.df_man_ven_matched_products = self.df_product[(self.df_product['Filter'] == 'Partial')]
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Partial')]

        # set aside everything that didn't match
        # but wait! there's more!
        self.df_product['VendorName'] = self.df_product[['VendorName_x']]
        self.df_product['ManufacturerName'] = self.df_product[['ManufacturerName_x']]
        self.df_product['VendorPartNumber'] = self.df_product[['VendorPartNumber_x']]
        self.df_product['FyProductNumber'] = self.df_product[['FyProductNumber_x']]

        drop_columns = ['Filter','db_IsDiscontinued','db_FyIsDiscontinued','ProductId','ProductPriceId','BaseProductPriceId',
                        'ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId','VAProductPriceId',
                        'ManufacturerName_x', 'ManufacturerName_y','VendorName_x','VendorName_y',
                        'FyProductNumber_x','FyProductNumber_y','VendorPartNumber_x','VendorPartNumber_y']

        self.df_product = self.df_product.drop(columns=drop_columns)

        if len(self.df_man_ven_matched_products.index) > 0:
            self.man_ven_cleanup()
            self.df_product_remains_new = self.df_man_ven_matched_products[(self.df_man_ven_matched_products['Filter'] == 'New')]
            self.df_man_ven_matched_products = self.df_man_ven_matched_products[(self.df_man_ven_matched_products['Filter'] != 'New')]

            if len(self.df_product_remains_new.columns) > 0:
                drop_new = ['Filter', 'ProductId', 'ProductPriceId', 'db_IsDiscontinued','db_FyIsDiscontinued', 'ECATProductPriceId',
                            'HTMEProductPriceId', 'GSAProductPriceId', 'VAProductPriceId','BaseProductPriceId']
                self.df_product_remains_new = self.df_product_remains_new.drop(columns=drop_new)

                self.df_product = pandas.concat([self.df_product, self.df_product_remains_new], ignore_index=True)

        print('Round 4')
        self.obProgressBarWindow.update_bar(5)
        # print(self.df_product.columns)
        self.df_price_agnostic_product_lookup['Filter'] = 'Partial'
        self.df_product = self.df_product.merge(self.df_price_agnostic_product_lookup, how='left',on=['FyCatalogNumber','ManufacturerName'])
        self.df_fy_cat_matched_products = self.df_product[(self.df_product['Filter'] == 'Partial')]

        self.df_product = self.df_product[(self.df_product['Filter'] != 'Partial')]

        if len(self.df_fy_cat_matched_products.index) > 0:
            self.fy_cat_cleanup()

        # print('b',self.df_product.columns)

        self.df_product['ManufacturerPartNumber'] = self.df_product[['ManufacturerPartNumber_x']]
        self.df_product['VendorPartNumber'] = self.df_product[['VendorPartNumber_x']]
        self.df_product['VendorName'] = self.df_product[['VendorName_x']]
        self.df_product['FyProductNumber'] = self.df_product[['FyProductNumber_x']]

        drop_columns = ['Filter',
                        'ManufacturerPartNumber_x', 'ManufacturerPartNumber_y','VendorName_x','VendorName_y',
                        'FyProductNumber_x','FyProductNumber_y','VendorPartNumber_x','VendorPartNumber_y']
        self.df_product = self.df_product.drop(columns=drop_columns)
        # print('c',self.df_product.columns)

        self.df_product['Filter'] = 'New'

        print('Collecting')
        self.obProgressBarWindow.update_bar(6)
        if len(self.df_full_matched_product.index) > 0:
            self.df_full_matched_product = self.df_full_matched_product.drop_duplicates()
            try:
                self.df_product = pandas.concat([self.df_product, self.df_full_matched_product], ignore_index = True)
            except pandas.errors.InvalidIndexError:
                print(self.df_full_matched_product)
                print(self.df_product)
                print('Invalid index error 2: this represents a bug')

        del self.df_full_matched_product

        if len(self.df_pricing_matched_product.index) > 0:
            self.df_pricing_matched_product = self.df_pricing_matched_product.drop_duplicates()
            try:
                self.df_product = pandas.concat([self.df_product,self.df_pricing_matched_product], ignore_index = True)
            except pandas.errors.InvalidIndexError:
                print(self.df_pricing_matched_product)
                print(self.df_product)
                print('Invalid index error 3: this represents a bug')

        del self.df_pricing_matched_product

        if len(self.df_man_ven_matched_products.index) > 0:
            self.df_man_ven_matched_products = self.df_man_ven_matched_products.drop_duplicates()
            try:
                self.df_product = pandas.concat([self.df_product,self.df_man_ven_matched_products], ignore_index = True)
            except pandas.errors.InvalidIndexError:
                print(self.df_man_ven_matched_products)
                print(self.df_product)
                print('Invalid index error 4: this represents a bug')

        del self.df_man_ven_matched_products

        if len(self.df_fy_cat_matched_products.index) > 0:
            self.df_fy_cat_matched_products = self.df_fy_cat_matched_products.drop_duplicates()
            try:
                self.df_product = pandas.concat([self.df_product, self.df_fy_cat_matched_products], ignore_index = True)
            except pandas.errors.InvalidIndexError:
                print(self.df_fy_cat_matched_products)
                print(self.df_product)
                print('Invalid index error 5: this represents a bug')

        del self.df_fy_cat_matched_products


        if 'VendorPartNumber_x' in self.df_product.columns and 'VendorPartNumber' not in self.df_product.columns:
            self.df_product['VendorPartNumber'] = self.df_product['VendorPartNumber_x']
            self.df_product = self.df_product.drop(columns = ['VendorPartNumber_x'])

        if 'FyProductNumber_x' in self.df_product.columns and 'FyProductNumber' not in self.df_product.columns:
            self.df_product['FyProductNumber'] = self.df_product['FyProductNumber_x']
            self.df_product = self.df_product.drop(columns = ['FyProductNumber_x'])

        print('Finals')
        self.obProgressBarWindow.update_bar(7)
        self.eval_cases()

        # self.duplicate_logic()
        if 'ProductPriceId' in self.df_product.columns:
            print('Adding Notes')
            self.df_product = self.df_product.merge(self.df_product_notes, how='left',on=['ProductPriceId'])
            self.obProgressBarWindow.update_bar(8)

        del self.df_product_notes
        self.df_product = self.df_product.reindex()
        self.obProgressBarWindow.update_bar(9)


    def merge_and_split(self, df_left, df_right, on_columns):
        df_left['is_left'] = '1'
        df_right['is_right'] = '1'

        left_headers = set(df_left.columns)
        right_headers = set(df_right.columns)

        left_only_headers = list(left_headers.difference(right_headers))
        right_only_headers = list(right_headers.difference(left_headers))

        df_result = df_left.merge(df_right, how='outer', on= on_columns)

        df_only_left = df_result[(df_result['is_left'] == '1')]
        df_center = df_only_left[(df_only_left['is_right'] == '1')]
        df_left = df_only_left[(df_only_left['is_right'] != '1')]

        df_right = df_result[(df_result['is_left'] != '1')]

        df_left = df_left.drop(columns = ['is_left','is_right']+right_only_headers)
        df_center = df_center.drop(columns = ['is_left','is_right'])
        df_right = df_right.drop(columns = ['is_left','is_right']+left_only_headers)


        return df_left, df_center, df_right


    def duplicate_logic(self):
        # it seems that this needs better returns for review
        # perhaps pull the
        # counts FyProductNumber occurance as series
        self.df_dupe_product = self.df_product.copy()

        cur_headers = set(self.df_dupe_product.columns)
        match_set = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber', 'FyProductNumber', 'VendorName', 'VendorPartNumber']

        matched_headers = list(cur_headers.difference(set(match_set)))
        self.df_dupe_product = self.df_dupe_product.drop(columns=matched_headers)
        self.df_dupe_product.drop_duplicates(['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber'], inplace= True)

        self.srs_matched_product = self.df_dupe_product.loc[:, 'FyProductNumber'].value_counts()

        self.srs_matched_product.rename_axis()

        # sets series to dataframe
        self.df_matched_product = self.srs_matched_product.to_frame().reset_index()
        # names columns in new dataframe
        self.df_matched_product.rename(columns = {'FyProductNumber':'number','index':'FyProductNumber'}, inplace = 1)

        # assign duplicate marker
        self.df_matched_product['is_duplicated'] = 'Y'
        self.df_matched_product = self.df_matched_product.loc[(self.df_matched_product['number'] > 1),['FyProductNumber','is_duplicated']]

        # merge the duplicate mark back in
        self.df_product = self.df_product.merge(self.df_matched_product, how='left', on='FyProductNumber')

        self.df_product.loc[(self.df_product['is_duplicated'] == 'Y'), 'Filter'] = 'Possible Duplicate'

        self.df_product = self.df_product.drop(columns = ['is_duplicated'])

        # here we are going to match everything called new to the existing manufcaturer parts
        # this is to indicate the difference between the ingestable new products and updatable products
        if 'db_FyIsDiscontinued' in self.df_product.columns:
            self.df_product.loc[(self.df_product['db_FyIsDiscontinued'] == 'Y'), 'Alert'] = 'This product is currently discontinued'


    def eval_cases(self):
        # include an alert here that says what it is
        self.df_product.loc[(self.df_product['Filter'] == 'Ready'), 'Alert'] = 'Ready to update/contract'
        self.df_product.loc[(self.df_product['Filter'] == 'Base Pricing'), 'Alert'] = 'These go through update base pricing'
        self.df_product.loc[(self.df_product['Filter'] == 'Partial'), 'Alert'] = 'These can go through update step 1.5'

        self.df_product.loc[(self.df_product['Filter'] == 'case_1'), 'Alert'] = 'Vendor Part Number Change'
        self.df_product.loc[(self.df_product['Filter'] == 'case_1'), 'Filter'] = 'VendorPartNumberChange'

        # some of them are going to be changed
        self.df_product.loc[(self.df_product['Filter'] == 'case_2'), 'Alert'] = 'New Vendor for Existing Configuration(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_2'), 'Filter'] = 'Partial'

        self.df_product.loc[(self.df_product['Filter'] == 'case_3'), 'Alert'] = 'New Vendor for New Configuration(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_3'), 'Filter'] = 'Partial'

        self.df_product.loc[(self.df_product['Filter'] == 'case_6'), 'Alert'] = 'New Vendor for Existing Configuration(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_6'), 'Filter'] = 'Partial'


        self.df_product.loc[(self.df_product['Filter'] == 'case_4'), 'Alert'] = 'Configuration change-4'
        self.df_product.loc[(self.df_product['Filter'] == 'case_7'), 'Alert'] = 'Configuration change-7'
        self.df_product.loc[(self.df_product['Filter'] == 'case_7'), 'Filter'] = 'case_4'
        self.df_product.loc[(self.df_product['Filter'] == 'case_4'), 'Filter'] = 'ConfigurationChange'


        self.df_product.loc[(self.df_product['Filter'] == 'case_5'), 'Alert'] = 'Possible Override/Duplicate'
        self.df_product.loc[(self.df_product['Filter'] == 'case_5'), 'Filter'] = 'Possible_Duplicate'


        self.df_product.loc[(self.df_product['Filter'] == 'case_8'), 'Alert'] = 'New Vendor Existing product(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_8'), 'Filter'] = 'Partial'

        self.df_product.loc[(self.df_product['Filter'] == 'case_9'), 'Alert'] = 'ManufacturerName was corrected'
        self.df_product.loc[(self.df_product['Filter'] == 'case_9'), 'Filter'] = 'Ready'

        self.df_product.loc[(self.df_product['Filter'] == 'case_14'), 'Filter'] = 'manufacturer_part_number_change'

        self.df_product.loc[(self.df_product['Filter'] == 'check_vendor'), 'Alert'] = 'Verify your VendorName'

        self.df_product.loc[(self.df_product['Filter'] == 'vendor_part_number_change'), 'Alert'] = 'Vendor Part Number Change'
        self.df_product.loc[(self.df_product['Filter'] == 'manufacturer_part_number_change'), 'Alert'] = 'Manufacturer Part Number Change'
        self.df_product.loc[(self.df_product['Filter'] == 'check_vendor_and_manu_part'), 'Alert'] = 'Manufacturer Part Number Change, check Vendor'


        self.df_product.loc[(self.df_product['Filter'] == 'Possible_Duplicate'), 'TakePriority'] = 'A'

        self.df_product.loc[(self.df_product['Filter'] == 'case_4'), 'TakePriority'] = 'W'

        self.df_product.loc[(self.df_product['Filter'] == 'check_vendor_and_manu_part'), 'TakePriority'] = 'I'
        self.df_product.loc[(self.df_product['Filter'] == 'manufacturer_part_number_change'), 'TakePriority'] = 'H'
        self.df_product.loc[(self.df_product['Filter'] == 'vendor_part_number_change'), 'TakePriority'] = 'G'
        self.df_product.loc[(self.df_product['Filter'] == 'check_vendor'), 'TakePriority'] = 'F'

        self.df_product.loc[(self.df_product['Filter'] == 'New'), 'TakePriority'] = 'E'
        self.df_product.loc[(self.df_product['Filter'] == 'Partial'), 'TakePriority'] = 'D'
        self.df_product.loc[(self.df_product['Filter'] == 'case_1'), 'TakePriority'] = 'C'
        self.df_product.loc[(self.df_product['Filter'] == 'Base Pricing'), 'TakePriority'] = 'B'
        self.df_product.loc[(self.df_product['Filter'] == 'Ready'), 'TakePriority'] = 'A'

        self.df_product.loc[(self.df_product['BlockedManufacturer'] == 1), 'Filter'] = 'BlockedManufacturer'
        self.df_product.loc[(self.df_product['Filter'] == 'BlockedManufacturer'), 'Alert'] = 'This Manufacturer Name is blocked from processing.'

        self.df_product.sort_values(by=['FyCatalogNumber','FyProductNumber','VendorName','VendorPartNumber','ManufacturerName','ManufacturerPartNumber','TakePriority'] , inplace = True)

        self.df_product.drop_duplicates(['FyCatalogNumber','FyProductNumber','VendorName','VendorPartNumber','ManufacturerName','ManufacturerPartNumber'] , ignore_index = True, inplace = True)


    def man_ven_cleanup(self):
        # in this new version we will do far more

        # Ready
        base_price = ((self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] == self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] == self.df_man_ven_matched_products['FyProductNumber_y']) &
                  (self.df_man_ven_matched_products['BaseProductPriceId'] == 'Load Pricing'))

        # new vendor for new configuration
        case_3 = ((self.df_man_ven_matched_products['VendorName_x'] != self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] != self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] != self.df_man_ven_matched_products['FyProductNumber_y']))

        # configuration change
        case_4 = ( (self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y'] ) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] == self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] != self.df_man_ven_matched_products['FyProductNumber_y']))

        # BAAAAAD manufacturer
        case_9 = ((self.df_man_ven_matched_products['ManufacturerName_x'] != self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y'] ) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] == self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] == self.df_man_ven_matched_products['FyProductNumber_y']))

        # BAAAAAD manufacturer
        case_10 = ((self.df_man_ven_matched_products['VendorName_x'] != self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] == self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] == self.df_man_ven_matched_products['FyProductNumber_y']))

        # BAAAAAD manufacturer
        case_11 = ((self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] != self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] == self.df_man_ven_matched_products['FyProductNumber_y']))


        conditions = [base_price,case_4,case_9,case_3,case_10,case_11]
        choices = ['Base Price','case_4','case_9','case_3','check_vendor','vendor_part_number_change']

        self.df_man_ven_matched_products['Filter'] = np.select(conditions, choices, default='New')

        self.df_man_ven_matched_products.sort_values(by=['FyCatalogNumber','ManufacturerName_x','ManufacturerPartNumber','FyProductNumber_x','VendorName_x','VendorPartNumber_x','Filter'] , inplace = True, ascending=False)
        self.df_man_ven_matched_products.drop_duplicates(['FyCatalogNumber','ManufacturerName_x','ManufacturerPartNumber','FyProductNumber_x','VendorName_x','VendorPartNumber_x'] , ignore_index = True, inplace = True)

        self.df_man_ven_matched_products = self.man_ven_match_x_y_cleaning(self.df_man_ven_matched_products)


    def fy_cat_cleanup(self):
        #  matched on 'FyCatalogNumber','ManufacturerName'
        # This is a likely override/duplicate
        case_5 = ((self.df_fy_cat_matched_products['VendorName_x'] == self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] != self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] != self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] == self.df_fy_cat_matched_products['FyProductNumber_y']))

        # new vendor exising configuration
        case_6 = ((self.df_fy_cat_matched_products['VendorName_x'] != self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] == self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] != self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] == self.df_fy_cat_matched_products['FyProductNumber_y']))

        # Configuration change
        case_7 = ((self.df_fy_cat_matched_products['VendorName_x'] == self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] == self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] == self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] != self.df_fy_cat_matched_products['FyProductNumber_y']))

        # True partial honestly, these shouldn't happen
        case_8 = ((self.df_fy_cat_matched_products['VendorName_x'] != self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] == self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] != self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] != self.df_fy_cat_matched_products['FyProductNumber_y']))

        case_12 = ((self.df_fy_cat_matched_products['VendorName_x'] == self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] != self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] == self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] == self.df_fy_cat_matched_products['FyProductNumber_y']))

        case_13 = ((self.df_fy_cat_matched_products['VendorName_x'] != self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] != self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] == self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] == self.df_fy_cat_matched_products['FyProductNumber_y']))

        # new vendor exising configuration
        case_14 = ((self.df_fy_cat_matched_products['VendorName_x'] != self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] != self.df_fy_cat_matched_products['ManufacturerPartNumber_y']))

        conditions = [case_5,case_6,case_7,case_8,case_12,case_13,case_14]
        choices = ['case_5','case_6','case_7','case_8','manufacturer_part_number_change','check_vendor_and_manu_part','case_14']

        self.df_fy_cat_matched_products['Filter'] = np.select(conditions, choices, default='Partial')


        self.df_fy_cat_matched_products.sort_values(by=['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x','Filter'] , inplace = True)
        self.df_fy_cat_matched_products.drop_duplicates(['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x'] , ignore_index = True, inplace = True)

        self.df_fy_cat_matched_products =self.x_y_cleaning(self.df_fy_cat_matched_products)


    def man_ven_match_x_y_cleaning(self, df_to_clean):
        # good: FyCatalogNumber, ManufacturerPartNumber, ProductId
        df_to_clean_case_1 = df_to_clean[(df_to_clean['Filter'] == 'Base Price')].copy()
        df_to_clean_case_3 = df_to_clean[(df_to_clean['Filter'] == 'case_3')].copy()
        df_to_clean_case_4 = df_to_clean[(df_to_clean['Filter'] == 'case_4')].copy()
        df_to_clean_case_9 = df_to_clean[(df_to_clean['Filter'] == 'case_9')].copy()

        df_to_clean_check_vendor = df_to_clean[(df_to_clean['Filter'] == 'check_vendor')].copy()
        df_to_clean_vendor_part = df_to_clean[(df_to_clean['Filter'] == 'vendor_part_number_change')].copy()

        df_new = df_to_clean[(df_to_clean['Filter'] == 'New')].copy()

        if len(df_to_clean_case_1.index) > 0:
            # vendor part number change
            df_to_clean_case_1['ManufacturerName'] = df_to_clean_case_1[['ManufacturerName_x']]
            df_to_clean_case_1['FyProductNumber'] = df_to_clean_case_1[['FyProductNumber_x']]
            df_to_clean_case_1['VendorName'] = df_to_clean_case_1[['VendorName_x']]
            df_to_clean_case_1['VendorPartNumber'] = df_to_clean_case_1[['VendorPartNumber_x']]

        drop_1 = ['BaseProductPriceId','ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y',
                  'ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId', 'VAProductPriceId']
        df_to_clean_case_1 = df_to_clean_case_1.drop(columns = drop_1)

        if len(df_to_clean_case_3.index) > 0:
            # new vendor for new configuration
            df_to_clean_case_3['ManufacturerName'] = df_to_clean_case_3[['ManufacturerName_x']]
            df_to_clean_case_3['FyProductNumber'] = df_to_clean_case_3[['FyProductNumber_x']]
            df_to_clean_case_3['VendorName'] = df_to_clean_case_3[['VendorName_x']]
            df_to_clean_case_3['VendorPartNumber'] = df_to_clean_case_3[['VendorPartNumber_x']]

        drop_3 = ['ProductPriceId','BaseProductPriceId', 'ManufacturerName_x', 'FyProductNumber_x', 'VendorName_x',
                      'VendorPartNumber_x', 'VendorPartNumber_y', 'VendorName_y', 'FyProductNumber_y', 'ManufacturerName_y',
                  'ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId', 'VAProductPriceId']
        df_to_clean_case_3 = df_to_clean_case_3.drop(columns=drop_3)

        if len(df_to_clean_case_4.index) > 0:
            # configuration change
            df_to_clean_case_4['ManufacturerName'] = df_to_clean_case_4[['ManufacturerName_x']]
            df_to_clean_case_4['FyProductNumber'] = df_to_clean_case_4[['FyProductNumber_x']]
            df_to_clean_case_4['CurrentFyProductNumber'] = df_to_clean_case_4[['FyProductNumber_y']]
            df_to_clean_case_4['VendorName'] = df_to_clean_case_4[['VendorName_x']]
            df_to_clean_case_4['VendorPartNumber'] = df_to_clean_case_4[['VendorPartNumber_x']]

        drop_4 = ['ProductPriceId','BaseProductPriceId', 'ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y','ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId', 'VAProductPriceId']

        df_to_clean_case_4 = df_to_clean_case_4.drop(columns = drop_4)

        if len(df_to_clean_case_9.index) > 0:
            # configuration change
            df_to_clean_case_9['ManufacturerName'] = df_to_clean_case_9[['ManufacturerName_y']]
            df_to_clean_case_9['PossibleSupplierName'] = df_to_clean_case_9[['ManufacturerName_x']]
            df_to_clean_case_9['FyProductNumber'] = df_to_clean_case_9[['FyProductNumber_x']]
            df_to_clean_case_9['VendorName'] = df_to_clean_case_9[['VendorName_x']]
            df_to_clean_case_9['VendorPartNumber'] = df_to_clean_case_9[['VendorPartNumber_x']]

        drop_9 = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_case_9 = df_to_clean_case_9.drop(columns = drop_9)

        if len(df_to_clean_check_vendor.index) > 0:
            # configuration change
            df_to_clean_check_vendor['ManufacturerName'] = df_to_clean_check_vendor[['ManufacturerName_y']]
            df_to_clean_check_vendor['FyProductNumber'] = df_to_clean_check_vendor[['FyProductNumber_x']]
            df_to_clean_check_vendor['VendorName'] = df_to_clean_check_vendor[['VendorName_x']]
            df_to_clean_check_vendor['PossibleVendorName'] = df_to_clean_check_vendor[['VendorName_y']]
            df_to_clean_check_vendor['VendorPartNumber'] = df_to_clean_check_vendor[['VendorPartNumber_x']]

        drop_check_vendor = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_check_vendor = df_to_clean_check_vendor.drop(columns = drop_check_vendor)

        if len(df_to_clean_vendor_part.index) > 0:
            # configuration change
            df_to_clean_vendor_part['ManufacturerName'] = df_to_clean_vendor_part[['ManufacturerName_y']]
            df_to_clean_vendor_part['FyProductNumber'] = df_to_clean_vendor_part[['FyProductNumber_x']]
            df_to_clean_vendor_part['VendorName'] = df_to_clean_vendor_part[['VendorName_x']]
            df_to_clean_vendor_part['VendorPartNumber'] = df_to_clean_vendor_part[['VendorPartNumber_x']]
            df_to_clean_vendor_part['PossibleVendorPartNumber'] = df_to_clean_vendor_part[['VendorPartNumber_y']]

        drop_vendor_part = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_vendor_part = df_to_clean_vendor_part.drop(columns = drop_vendor_part)



        df_new['ManufacturerName'] = df_new[['ManufacturerName_x']]
        df_new['FyProductNumber'] = df_new[['FyProductNumber_x']]
        df_new['VendorName'] = df_new[['VendorName_x']]
        df_new['VendorPartNumber'] = df_new[['VendorPartNumber_x']]

        drop_p = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x', 'VendorPartNumber_y',
                  'VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_new = df_new.drop(columns = drop_p)

        df_to_return = pandas.concat([df_to_clean_case_1,df_new,df_to_clean_case_3,df_to_clean_case_4,df_to_clean_case_9,df_to_clean_check_vendor,df_to_clean_vendor_part],ignore_index=True)


        return df_to_return


    def x_y_cleaning(self, df_to_clean):
        #  matched on 'FyCatalogNumber','ManufacturerName'
        df_to_clean_case_5 = df_to_clean[(df_to_clean['Filter'] == 'case_5')].copy()
        df_to_clean_case_6 = df_to_clean[(df_to_clean['Filter'] == 'case_6')].copy()
        df_to_clean_case_7 = df_to_clean[(df_to_clean['Filter'] == 'case_7')].copy()
        df_to_clean_case_8 = df_to_clean[(df_to_clean['Filter'] == 'case_8')].copy()
        df_to_clean_case_14 = df_to_clean[(df_to_clean['Filter'] == 'case_14')].copy()
        df_to_clean_manu_part = df_to_clean[(df_to_clean['Filter'] == 'manufacturer_part_number_change')].copy()
        df_to_clean_vendor_manu_part = df_to_clean[(df_to_clean['Filter'] == 'check_vendor_and_manu_part')].copy()
        df_partials = df_to_clean[(df_to_clean['Filter'] == 'Partial')].copy()

        if len(df_to_clean_case_5.index) > 0:
            # This is a likely override/duplicate
            df_to_clean_case_5['VendorName'] = df_to_clean_case_5[['VendorName_x']]
            df_to_clean_case_5['ManufacturerPartNumber'] = df_to_clean_case_5[['ManufacturerPartNumber_x']]
            df_to_clean_case_5['Other_ManufacturerPartNumber'] = df_to_clean_case_5[['ManufacturerPartNumber_y']]
            df_to_clean_case_5['VendorPartNumber'] = df_to_clean_case_5[['VendorPartNumber_x']]
            df_to_clean_case_5['Other_VendorPartNumber'] = df_to_clean_case_5[['VendorPartNumber_y']]
            df_to_clean_case_5['FyProductNumber'] = df_to_clean_case_5[['FyProductNumber_x']]

        drop_5 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorName_y','VendorPartNumber_x',
                      'VendorPartNumber_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_5 = df_to_clean_case_5.drop(columns = drop_5)

        if len(df_to_clean_case_6.index) > 0:
            #  matched on 'FyCatalogNumber','ManufacturerName'
            # new vendor for existing configuration
            df_to_clean_case_6['FyProductNumber'] = df_to_clean_case_6[['FyProductNumber_x']]
            df_to_clean_case_6['VendorName'] = df_to_clean_case_6[['VendorName_x']]
            df_to_clean_case_6['VendorPartNumber'] = df_to_clean_case_6[['VendorPartNumber_x']]
            df_to_clean_case_6['ManufacturerPartNumber'] = df_to_clean_case_6[['ManufacturerPartNumber_x']]

        drop_6 = ['ProductPriceId','BaseProductPriceId','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                  'db_IsDiscontinued', 'db_FyIsDiscontinued',
                  'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_x','ManufacturerPartNumber_y',
                  'ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId', 'VAProductPriceId']
        df_to_clean_case_6 = df_to_clean_case_6.drop(columns = drop_6)

        if len(df_to_clean_case_7.index) > 0:
            # configuration change
            df_to_clean_case_7['ManufacturerPartNumber'] = df_to_clean_case_7[['ManufacturerPartNumber_x']]
            df_to_clean_case_7['FyProductNumber'] = df_to_clean_case_7[['FyProductNumber_x']]
            df_to_clean_case_7['CurrentFyProductNumber'] = df_to_clean_case_7[['FyProductNumber_y']]
            df_to_clean_case_7['VendorName'] = df_to_clean_case_7[['VendorName_x']]
            df_to_clean_case_7['VendorPartNumber'] = df_to_clean_case_7[['VendorPartNumber_x']]

        drop_7 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_7 = df_to_clean_case_7.drop(columns = drop_7)

        #  matched on 'FyCatalogNumber','ManufacturerName'
        if len(df_to_clean_case_8.index) > 0:
            df_to_clean_case_8['VendorName'] = df_to_clean_case_8[['VendorName_x']]
            df_to_clean_case_8['ManufacturerPartNumber'] = df_to_clean_case_8[['ManufacturerPartNumber_x']]
            df_to_clean_case_8['VendorName'] = df_to_clean_case_8[['VendorName_x']]
            df_to_clean_case_8['VendorPartNumber'] = df_to_clean_case_8[['VendorPartNumber_x']]
            df_to_clean_case_8['FyProductNumber'] = df_to_clean_case_8[['FyProductNumber_x']]

        drop_8 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_8 = df_to_clean_case_8.drop(columns = drop_8)

        #  matched on 'FyCatalogNumber','ManufacturerName'
        if len(df_to_clean_manu_part.index) > 0:
            df_to_clean_manu_part['VendorName'] = df_to_clean_manu_part[['VendorName_x']]
            df_to_clean_manu_part['ManufacturerPartNumber'] = df_to_clean_manu_part[['ManufacturerPartNumber_x']]
            df_to_clean_manu_part['PossibleManufacturerPartNumber'] = df_to_clean_manu_part[['ManufacturerPartNumber_y']]
            df_to_clean_manu_part['VendorName'] = df_to_clean_manu_part[['VendorName_x']]
            df_to_clean_manu_part['VendorPartNumber'] = df_to_clean_manu_part[['VendorPartNumber_x']]
            df_to_clean_manu_part['FyProductNumber'] = df_to_clean_manu_part[['FyProductNumber_x']]

        drop_manu_part = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_manu_part = df_to_clean_manu_part.drop(columns = drop_manu_part)

        #  matched on 'FyCatalogNumber','ManufacturerName'
        if len(df_to_clean_vendor_manu_part.index) > 0:
            df_to_clean_vendor_manu_part['VendorName'] = df_to_clean_vendor_manu_part[['VendorName_x']]
            df_to_clean_vendor_manu_part['ManufacturerPartNumber'] = df_to_clean_vendor_manu_part[['ManufacturerPartNumber_x']]
            df_to_clean_vendor_manu_part['PossibleManufacturerPartNumber'] = df_to_clean_vendor_manu_part[['ManufacturerPartNumber_y']]
            df_to_clean_vendor_manu_part['VendorName'] = df_to_clean_vendor_manu_part[['VendorName_x']]
            df_to_clean_vendor_manu_part['PossibleVendorName'] = df_to_clean_vendor_manu_part[['VendorName_y']]
            df_to_clean_vendor_manu_part['VendorPartNumber'] = df_to_clean_vendor_manu_part[['VendorPartNumber_x']]
            df_to_clean_vendor_manu_part['FyProductNumber'] = df_to_clean_vendor_manu_part[['FyProductNumber_x']]

        drop_vendor_manu_part = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_vendor_manu_part = df_to_clean_vendor_manu_part.drop(columns = drop_vendor_manu_part)


        if len(df_to_clean_case_14.index) > 0:
            # This is a likely override/duplicate
            df_to_clean_case_14['VendorName'] = df_to_clean_case_14[['VendorName_x']]
            df_to_clean_case_14['ManufacturerPartNumber'] = df_to_clean_case_14[['ManufacturerPartNumber_x']]
            df_to_clean_case_14['Other_ManufacturerPartNumber'] = df_to_clean_case_14[['ManufacturerPartNumber_y']]
            df_to_clean_case_14['VendorPartNumber'] = df_to_clean_case_14[['VendorPartNumber_x']]
            df_to_clean_case_14['Other_VendorPartNumber'] = df_to_clean_case_14[['VendorPartNumber_y']]
            df_to_clean_case_14['FyProductNumber'] = df_to_clean_case_14[['FyProductNumber_x']]

        drop_14 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorName_y','VendorPartNumber_x',
                      'VendorPartNumber_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_14 = df_to_clean_case_14.drop(columns = drop_14)



        df_partials['ManufacturerPartNumber'] = df_partials[['ManufacturerPartNumber_x']]
        df_partials['FyProductNumber'] = df_partials[['FyProductNumber_x']]
        df_partials['VendorName'] = df_partials[['VendorName_x']]
        df_partials['VendorPartNumber'] = df_partials[['VendorPartNumber_x']]

        drop_p = ['ProductPriceId','BaseProductPriceId',
                  'ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId', 'VAProductPriceId','ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x',
                  'VendorPartNumber_x','VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_partials = df_partials.drop(columns = drop_p)

        df_to_return = pandas.concat([df_partials, df_to_clean_case_5,df_to_clean_case_6,df_to_clean_case_7,df_to_clean_case_8,df_to_clean_manu_part,df_to_clean_vendor_manu_part,df_to_clean_case_14],ignore_index=True)

        return df_to_return


    def begin_process(self):
        self.success = False
        if self.is_viable:
            self.success, self.message = self.run_process()
        elif self.message == 'No message':
            fail_report = ''
            fail_message = ''

            if len(self.lst_untranslated_headers) > 0:
                untranslated_string = str(self.lst_untranslated_headers)
                untranslated_string = untranslated_string.replace(']','')
                untranslated_string = untranslated_string.replace('[','')
                untranslated_string = untranslated_string.replace('\'','')
                self.df_product['Untranslated Headers'] = untranslated_string
                self.message = 'The file has untranslatable headers, see output'

            missing_heads = self.get_missing_heads()
            if len(missing_heads) > 0:
                missing_string = str(missing_heads)
                missing_string = missing_string.replace(']','')
                missing_string = missing_string.replace('[','')
                missing_string = missing_string.replace('\'','')
                self.df_product['Missing Headers'] = missing_string

            if len(missing_heads) == 1:
                self.df_product['Fail'] = 'Failed due to missing headers'
                self.message = 'The file is missing a product field: {0}, see output'.format(missing_heads[0])
            elif len(missing_heads) != 0:
                self.df_product['Fail'] = 'Failed due to missing headers'
                self.message = 'The file is missing product fields: {0} and {1} more, see output'.format(missing_heads[0],
                                                                                     str(len(missing_heads) - 1))
            elif len(self.lst_untranslated_headers) > 0:
                self.df_product['Fail'] = 'Failed due to untranslated headers'
                self.message = 'The file has untranslatable headers, see output'
            else:
                self.df_product['Fail'] = 'Failed due to missing headers'
                self.message = 'The file is missing at least 1 supporting field, see output'

        return self.success, self.message

    def batch_preprocessing(self):
        pass

    def trigger_ingest_cleanup(self):
        pass

    def normalize_units(self, units):
        dct_uoi_map = {'BG':['BG','BAG','BAGS'],'BO':['BO'],'BT':['BT','BOTTLE','BOTTLES'],'BX':['BX','BOX','BOXES'],'CA':['CA','CS','CASE','CASES'],'CT':['CT','CARTON','CARTONS'],'DA':['DA'],'DR':['DR','DRUM','DRUMS'],'DZ':['DZ','DOZEN','DOZENS'],'EA':['EA','EACH','EACHES','ITEM','ITEMS','TEST','TESTS','TST','TSTS','PC','PCS','PIECE','PIECES'],'FT':['FT','FOOT'],'GA':['GA','GALLON'],'GL':['GL','GRAMSPERLITER'],'GM':['GM','GRAMSPERSQ.METER'],'GR':['GR','GRAM'],'HR':['HR','HOURS'],'IN':['IN','INCH'],'JR':['JR','JAR','JARS'],'KG':['KG','KILOGRAM'],'KT':['KT','KIT','KITS'],'LB':['LB','POUND'],'MO':['MO','MONTHS'],'MR':['MR','METER'],'OZ':['OZ','OUNCE'],'PK':['PK','PAK','PACK','PACKS','PACKAGE','PACKAGES','PKGS','PKG'],'PL':['PL','PALLET'],'PR':['PR','PAIR','PAIRS'],'PT':['PT','PINT'],'RK':['RK','ROLL-METRIC'],'RL':['RL','ROLL','ROLLS'],'RM':['RM','REAM'],'SE':['SE'],'SP':['SP','SHELFPACKAGE'],'ST':['ST','SET','SETS'],'TB':['TB','TUBE'],'VI':['VI','VIAL'],'YD':['YD','YARD'],'YR':['YR']}
        dct_uoi_translator = {}
        for each_key in dct_uoi_map:
            for each_val in dct_uoi_map[each_key]:
                dct_uoi_translator[each_val] = each_key

        units = units.upper()
        try:
            translated_uoi = dct_uoi_translator[units]
        except Exception as e:
            translated_uoi = units
        return translated_uoi

    def row_check(self, row, name_to_check):
        try:
            name_value = row[name_to_check]
            return True, name_value
        except KeyError:
            self.obReporter.update_report('Alert', '{0} was missing.'.format(name_to_check))
            return False, 0


    def float_check(self, float_name_val, report_name):
        try:
            checked_float_value = float(float_name_val)
            if checked_float_value >= 0:
                return True, checked_float_value
            else:
                self.obReporter.update_report('Alert', '{0} must be a positive number.'.format(report_name))
                return False, checked_float_value

        except TypeError:
            self.obReporter.update_report('Alert', '{0} must be a positive number.'.format(report_name))
            return False, 0

        except ValueError:
            return False, float_name_val

    def run_process(self):
        self.obReporter = ReporterObject()
        self.set_progress_bar(10, 'Batch preprocessing')
        self.obProgressBarWindow.update_unknown()
        self.manufacturer_repeater = {}
        self.batch_preprocessing()
        self.obProgressBarWindow.close()

        count_of_items = len(self.df_product.index)
        self.return_df_product = pandas.DataFrame(columns=self.out_column_headers)
        self.collect_return_dfs = []
        self.set_progress_bar(count_of_items, self.name)
        self.obProgressBarWindow.update_unknown()
        p_bar = 0
        good = 0
        bad = 0

        for colName, row in self.df_product.iterrows():
            # this takes one row and builds a df for a single product
            df_line_product = row.to_frame().T
            # this replaces empty string values with nan
            df_line_product = df_line_product.replace(r'^\s*$', self.np_nan, regex=True)
            # this removes all columns with all nan
            df_line_product = df_line_product.dropna(axis=1,how='all')

            if self.line_viability(df_line_product):
                self.ready_report(df_line_product)
                self.obReporter.report_line_viability(True)

                success, return_df_line_product = self.process_product_line(df_line_product)
                self.obReporter.final_report(success)

            else:
                self.obReporter.report_line_viability(False)
                success, return_df_line_product = self.report_missing_data(df_line_product)

            # appends all the product objects into a list
            report_set = self.obReporter.get_report()
            if 'Pass' in return_df_line_product.columns:
                return_df_line_product = return_df_line_product.drop(columns='Pass')

            if 'Alert' in return_df_line_product.columns:
                return_df_line_product = return_df_line_product.drop(columns='Alert')

            if 'Fail' in return_df_line_product.columns:
                return_df_line_product = return_df_line_product.drop(columns='Fail')

            return_df_line_product.insert(1, 'Pass', report_set[0])
            return_df_line_product.insert(2, 'Alert', report_set[1])
            return_df_line_product.insert(3, 'Fail', report_set[2])

            self.obReporter.clear_reports()

            self.collect_return_dfs.append(return_df_line_product)

            if success:
                good += 1
            else:
                bad += 1

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.trigger_ingest_cleanup()

        self.set_progress_bar(10,'Appending data...')
        self.obProgressBarWindow.update_unknown()

        # this uses df.append to combine all the df product objects together
        try:
            self.return_df_product = self.return_df_product.append(self.collect_return_dfs)
        except pandas.errors.InvalidIndexError:
            print('Invalid index error 1: this represents a bug')

        if self.set_new_order:
            matched_header_set = set(self.out_column_headers).union(set(self.return_df_product.columns))
            self.return_df_product = self.return_df_product[matched_header_set]

        self.obProgressBarWindow.close()

        self.df_product = self.return_df_product
        self.message = '{2}: {0} Fail, {1} Pass.'.format(bad,good,self.name)
        if good != 0:
            self.success = True

        return self.success, self.message

    def ready_report(self, df_line_product):
        pass_report = ''
        alert_report = ''
        fail_report = ''
        if 'Pass' in df_line_product.columns:
            pass_report = str(df_line_product['Pass'].values[0])

        if 'Alert' in df_line_product.columns:
            alert_report = str(df_line_product['Alert'].values[0])

        if 'Fail' in df_line_product.columns:
            fail_report = str(df_line_product['Fail'].values[0])

        self.obReporter.set_reports(pass_report,alert_report,fail_report)


    def process_product_line(self, df_line_product):
        self.obReporter.report_no_process()
        return False, df_line_product

    def process_boolean(self, row, isCol):
        try:
            test_val = row[isCol]
        except KeyError:
            return False, isCol

        try:
            test_val = int(test_val)
        except ValueError:
            if test_val.lower() in ['n','no']:
                test_val = 0
            elif test_val.lower() in ['y','yes']:
                test_val = 1
            else:
                test_val = -1

        if test_val not in [0, 1]:
            self.obReporter.update_report('Alert', 'Review {0}'.format(isCol))
            return False, test_val

        return True, test_val

    def process_attribute_data(self,df_line_product):
        df_collect_ids = df_line_product.copy()
        participant_attributes = list(set(self.att_fields).intersection(set(df_line_product.columns)))
        df_line_attributes = pandas.DataFrame(df_line_product[participant_attributes])

        if len(df_line_attributes.columns) > 0:
            for colName, row in df_line_attributes.iterrows():
                new_colName = row.index[0] + 'Id'
                if (new_colName not in row):
                    # this should be in validation as units validation or similar
                    term = self.obValidator.imperial_validation(row[row.index[0]])
                    if len(term) > 128:
                        term = term[:128]
                    new_id = self.obIngester.ingest_attribute(term,row.index[0])
                    df_collect_ids[new_colName] = [new_id]

        return df_collect_ids


    def process_manufacturer(self, df_collect_product_base_data, row):
        manufacturer = row['ManufacturerName']
        manufacturer = manufacturer.strip().replace('  ',' ')

        if 'UnitOfIssue' in row:
            unit_of_issue = self.normalize_units(row['UnitOfIssue'])

        elif 'FyUnitOfIssue' in row:
            unit_of_issue = self.normalize_units(row['FyUnitOfIssue'])

        else:
            unit_of_issue = 'EA'
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]
            self.obReporter.default_uoi_report()


        if manufacturer in self.manufacturer_repeater:
            new_manufacturer_id = self.manufacturer_repeater[manufacturer][0]
            new_prefix = self.manufacturer_repeater[manufacturer][1]
            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]
            return True, df_collect_product_base_data, new_prefix

        if 'ManufacturerId' in row:
            new_manufacturer_id = row['ManufacturerId']
            new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']].values[0][0]

            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix


        if manufacturer in self.manufacturer_repeater:
            new_manufacturer_id = self.manufacturer_repeater[manufacturer][0]
            new_prefix = self.manufacturer_repeater[manufacturer][1]
            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]
            return True, df_collect_product_base_data, new_prefix


        if (manufacturer.lower() in self.df_manufacturer_translator['SupplierName'].values):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['SupplierName'] == manufacturer.lower()), ['ManufacturerId',
                                                                                    'FyManufacturerPrefix']].values[0]

            self.manufacturer_repeater[manufacturer] = [new_manufacturer_id, new_prefix]
            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]
            return True, df_collect_product_base_data, new_prefix

        elif (manufacturer.upper() in self.df_manufacturer_translator['ManufacturerName'].unique()):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerName'] == manufacturer.upper()), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                0]

            self.manufacturer_repeater[manufacturer] = [new_manufacturer_id, new_prefix]
            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix


        elif 'SupplierName' in row:
            supplier = row['SupplierName'].lower()
            if (supplier in self.df_manufacturer_translator['SupplierName'].values):
                new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == supplier), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                    0]

                self.manufacturer_repeater[supplier] = [new_manufacturer_id, new_prefix]
                df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
                df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

                return True, df_collect_product_base_data, new_prefix
            else:
                return False, df_collect_product_base_data, '0000'

        else:
            return False, df_collect_product_base_data, '0000'


    def test_product_number(self, fy_product_number):
        if ' ' not in fy_product_number:
            return True
        else:
            test_unit = fy_product_number.partition(' ')[2]
            if test_unit not in self.valid_product_units:
                return False

        return True



    def build_part_number(self, row, manufacturer_part_number, fy_manufacturer_prefix, unit_of_issue, b_override):

        if 'FyCatalogNumber' not in row and 'FyCatalogNumber_y' not in row:
            fy_catalog_number = self.make_fy_catalog_number(fy_manufacturer_prefix, manufacturer_part_number, b_override)

        elif 'FyCatalogNumber' in row:
            fy_catalog_number = row['FyCatalogNumber']

        elif 'FyCatalogNumber_y' in row:
            fy_catalog_number = row['FyCatalogNumber_y']


        if 'FyProductNumber' not in row and 'FyProductNumber_y' not in row :

            fy_product_number = fy_catalog_number

            if unit_of_issue != 'EA':
                if fy_catalog_number[:-2] == unit_of_issue:
                    self.obReporter.update_report('Alert', 'Please check for duplicate units in FyProductNumber')
                fy_product_number = fy_catalog_number + ' ' + unit_of_issue

        elif 'FyProductNumber' in row:
            fy_product_number = row['FyProductNumber']

        elif 'FyProductNumber_y' in row:
            fy_product_number = row['FyProductNumber_y']

        return fy_catalog_number, fy_product_number


    def make_fy_catalog_number(self,prefix, manufacturer_part_number, b_override = False):
        if b_override:
            if len(manufacturer_part_number) >= 22:
                self.obReporter.update_report('Alert','Long Manufacturer Part Number')

            FY_catalog_number = str(prefix) + '-' + manufacturer_part_number.upper()
        else:
            clean_part_number = self.obValidator.clean_part_number(manufacturer_part_number)
            FY_catalog_number = str(prefix)+'-'+clean_part_number.upper()

        return FY_catalog_number


    def line_viability(self,df_product_line):
        # line viability checks
        line_headers = set(list(df_product_line.columns))
        required_headers = set(self.req_fields)
        return required_headers.issubset(line_headers)


    def report_missing_data(self, df_line_product):
        line_headers = set(list(df_line_product.columns))
        required_headers = set(self.req_fields)
        missing_headers = list(required_headers.difference(line_headers))
        report = 'Missing Data: ' + str(missing_headers)[1:-1]+'.'
        report = report.replace("\'",'')

        self.obReporter.update_report('Fail',report)
        return False, df_line_product


    def get_df(self):
        return self.df_product



class ReporterObject():
    def __init__(self):
        self.name = 'Lois Lane'
        self.fail_report = ''
        self.alert_report = ''
        self.pass_report = ''

    def update_report(self, report_type, report_text):
        report_types_allowed = ['Fail','Alert','Pass']
        if report_type == 'Fail':
            if report_text not in self.fail_report:
                if self.fail_report != '':
                    self.fail_report = self.fail_report+'; '+report_text
                else:
                    self.fail_report = report_text

        if report_type == 'Alert':
            if report_text not in self.alert_report:
                if self.alert_report != '':
                    self.alert_report = self.alert_report+'; '+report_text
                else:
                    self.alert_report = report_text

        if report_type == 'Pass':
            if report_text not in self.pass_report:
                if self.pass_report != '':
                    self.pass_report = self.pass_report+'; '+report_text
                else:
                    self.pass_report = report_text


    def get_report(self):
        return self.pass_report, self.alert_report, self.fail_report

    def set_reports(self,pass_report,alert_report,fail_report):
        self.fail_report = fail_report
        self.alert_report = alert_report
        self.pass_report = pass_report

    def clear_reports(self):
        self.fail_report = ''
        self.alert_report = ''
        self.pass_report = ''

    def report_no_process(self):
        self.update_report('Alert', 'No process built')

    def report_line_viability(self,is_good):
        if is_good:
            self.update_report('Pass', 'Passed Line Viability')
        else:
            self.update_report('Fail', 'Failed Line Viability')

    def final_report(self,is_good):
        if is_good:
            self.update_report('Pass', 'Success at exit')
        else:
            self.update_report('Fail', 'Failed at exit')

    def report_new_manufacturer(self):
        self.update_report('Fail', 'New Manufacturer must be ingested')

    def price_report(self,is_good):
        if is_good:
            self.update_report('Pass', 'Minumum product price success')
        else:
            self.update_report('Fail', 'Minumum product price failure')


    def fill_price_report(self,is_good):
        if is_good:
            self.update_report('Pass', 'Fill product price success')
        else:
            self.update_report('Fail', 'Fill product price failure')


    def default_uoi_report(self):
        self.update_report('Alert', 'Default UOI')





# future objects
# and format

class GSAPrice(BasicProcessObject):
    req_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product):
        super().__init__(df_product)
        self.name = 'GSA Price Ingestion'

    def process_product_line(self, return_df_line_product):
        return_df_line_product['Report'] = ['Process not built']
        return False, return_df_line_product

    # This will use the ingest function update_fks_base_price
    # as will all the other pricing pathways

## class add more here


def test_normalize_units(units):
    dct_uoi_map = {'BG':['BG','BAG','BAGS'],'BO':['BO'],'BT':['BT','BOTTLE','BOTTLES'],'BX':['BX','BOX','BOXES'],'CA':['CA','CS','CASE','CASES'],'CT':['CT','CARTON','CARTONS'],'DA':['DA'],'DR':['DR','DRUM','DRUMS'],'DZ':['DZ','DOZEN','DOZENS'],'EA':['EA','EACH','EACHES','ITEM','ITEMS','TEST','TESTS','TST','TSTS','PC','PCS','PIECE','PIECES'],'FT':['FT','FOOT'],'GA':['GA','GALLON'],'GL':['GL','GRAMSPERLITER'],'GM':['GM','GRAMSPERSQ.METER'],'GR':['GR','GRAM'],'HR':['HR','HOURS'],'IN':['IN','INCH'],'JR':['JR','JAR','JARS'],'KG':['KG','KILOGRAM'],'KT':['KT','KIT','KITS'],'LB':['LB','POUND'],'MO':['MO','MONTHS'],'MR':['MR','METER'],'OZ':['OZ','OUNCE'],'PK':['PK','PAK','PACK','PACKS','PACKAGE','PACKAGES','PKGS','PKG'],'PL':['PL','PALLET'],'PR':['PR','PAIR','PAIRS'],'PT':['PT','PINT'],'RK':['RK','ROLL-METRIC'],'RL':['RL','ROLL','ROLLS'],'RM':['RM','REAM'],'SE':['SE'],'SP':['SP','SHELFPACKAGE'],'ST':['ST','SET','SETS'],'TB':['TB','TUBE'],'VI':['VI','VIAL'],'YD':['YD','YARD'],'YR':['YR']}
    dct_uoi_translator = {}
    for each_key in dct_uoi_map:
        for each_val in dct_uoi_map[each_key]:
            dct_uoi_translator[each_val] = each_key

    units = units.upper()
    try:
        translated_uoi = dct_uoi_translator[units]
    except Exception as e:
        translated_uoi = units
    return translated_uoi


def test_frame():
    dct_uoi_map = {'BG':['BG', 'BAG', 'BAGS'], 'BO':['BO'], 'BT':['BT', 'BOTTLE', 'BOTTLES'],
                   'BX':['BX', 'BOX', 'BOXES'], 'CA':['CA', 'CS', 'CASE', 'CASES'], 'CT':['CT', 'CARTON', 'CARTONS'],
                   'DA':['DA'], 'DR':['DR', 'DRUM', 'DRUMS'], 'DZ':['DZ', 'DOZEN', 'DOZENS'],
                   'EA':['EA', 'EACH', 'EACHES', 'ITEM', 'ITEMS', 'TEST', 'TESTS', 'TST', 'TSTS', 'PC', 'PCS', 'PIECE',
                         'PIECES'], 'FT':['FT', 'FOOT'], 'GA':['GA', 'GALLON'], 'GL':['GL', 'GRAMSPERLITER'],
                   'GM':['GM', 'GRAMSPERSQ.METER'], 'GR':['GR', 'GRAM'], 'HR':['HR', 'HOURS'], 'IN':['IN', 'INCH'],
                   'JR':['JR', 'JAR', 'JARS'], 'KG':['KG', 'KILOGRAM'], 'KT':['KT', 'KIT', 'KITS'],
                   'LB':['LB', 'POUND'], 'MO':['MO', 'MONTHS'], 'MR':['MR', 'METER'], 'OZ':['OZ', 'OUNCE'],
                   'PK':['PK', 'PAK', 'PACK', 'PACKS', 'PACKAGE', 'PACKAGES', 'PKGS', 'PKG'], 'PL':['PL', 'PALLET'],
                   'PR':['PR', 'PAIR', 'PAIRS'], 'PT':['PT', 'PINT'], 'RK':['RK', 'ROLL-METRIC'],
                   'RL':['RL', 'ROLL', 'ROLLS'], 'RM':['RM', 'REAM'], 'SE':['SE'], 'SP':['SP', 'SHELFPACKAGE'],
                   'ST':['ST', 'SET', 'SETS'], 'TB':['TB', 'TUBE'], 'VI':['VI', 'VIAL'], 'YD':['YD', 'YARD'],
                   'YR':['YR']}
    test_uois = ['BG','BAG','BAGS','BO','BT','BOTTLE','BOTTLES','BX','BOX','BOXES','CA','CS','CASE','CASES','CT','CARTON','CARTONS','DA','DR','DRUM','DRUMS','DZ','DOZEN','DOZENS','EA','EACH','EACHES','ITEM','ITEMS','TEST','TESTS','TST','TSTS','PC','PCS','PIECE','PIECES','FT','FOOT','GA','GALLON','GL','GRAMSPERLITER','GM','GRAMSPERSQ.METER','GR','GRAM','HR','HOURS','IN','INCH','JR','JAR','JARS','KG','KILOGRAM','KT','KIT','KITS','LB','POUND','MO','MONTHS','MR','METER','OZ','OUNCE','PK','PAK','PACK','PACKS','PACKAGE','PACKAGES','PKGS','PKG','PL','PALLET','PR','PAIR','PAIRS','PT','PINT','RK','ROLL-METRIC','RL','ROLL','ROLLS','RM','REAM','SE','SP','SHELFPACKAGE','ST','SET','SETS','TB','TUBE','VI','VIAL','YD','YARD','YR']
    test_results = {}

    for each_uoi in test_uois:
        result_uoi = test_normalize_units(each_uoi)
        if result_uoi not in test_results:
            test_results[result_uoi] = [each_uoi]
        elif each_uoi not in test_results[result_uoi]:
            test_results[result_uoi].append(each_uoi)

    if test_results == dct_uoi_map:
        print("Pass")
    else:
        print(test_results)
        print(dct_uoi_map)
        print("Fail")



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    test_frame()




## end ##




