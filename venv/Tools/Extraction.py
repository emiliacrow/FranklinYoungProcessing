# CreatedBy: Emilia Crow
# CreateDate: 20210520
# Updated: 20210602
# CreateFor: Franklin Young International

import re

class Extractor:
    def __init__(self):
        self.name = 'Enrique the Extractor'
        self.lst_extractions = []
        self.pat_for_clean = '(\{\d+\})'
        # simple patterns to be used wherever
        self.pat_for_decimal = '(\d{0,3}[\.,]?\d{0,3}[\.,]?\d{1,4})'
        self.pat_for_fractions = '(\d{0,3}[-/]?\d{0,3}[-/]?\d{1,4})'
        self.pat_for_drop_val = '(?<=[\D,^])(\s?0{1,3})(?!\.\d)'

        self.pat_for_uom = '(CS|BX|CT|DZ|EA|MO|PK|RL|RM|TB|BG|GR|JR|KT|PR)'
        self.pat_for_uom_word = '(refill|piece|card|pack|case|each|bag|box|ctn|tip)'
        self.pat_for_uom_words = '(refills|pieces|eaches|boxes|packs|cases|cards|bags|tips|pcs)'
        self.pat_for_uom_all_words = '(refills|refill|pieces|eaches|piece|boxes|packs|cases|cards|card|pack|case|each|bags|tips|bag|box|ctn|tip|pcs)'

        self.pat_for_uoi = '({0}/{1})'.format(self.pat_for_decimal, self.pat_for_uom)

        # add '100 per box' to this set?

        # VWR STOPPER RUB 1HOLE (1LB)  1PK=69
        # Capture: 1PK=69
        self.pat_for_uoi_1 = '[ \(]({0}\s?{1}\={0})'.format(self.pat_for_decimal, self.pat_for_uom)
        # LAB COAT WHITE BUTTON MENS XL  10EA=CS, 1EA/CS
        # Capture: 10EA=CS, 1EA/CS
        self.pat_for_uoi_2 = '[ \(]({0}\s?{1}[\=/]{1})'.format(self.pat_for_decimal, self.pat_for_uom)
        # LAB COAT WHITE BUTTON MENS XL  1EA/25CS
        # Capture: 1EA/25CS
        self.pat_for_uoi_3 = '[ \(]({0}\s?{1}/{0}\s?{1})'.format(self.pat_for_decimal, self.pat_for_uom)
        # Standard HGA Graphite Walled Tube, 24ea/BOX
        # Capture: 24ea/BOX
        self.pat_for_uoi_4 = '[ \(]({0}\s?{1}/{2})'.format(self.pat_for_decimal, self.pat_for_uom, self.pat_for_uom_word)

        # Vwr Pipet Tip Univ Reload St 300Ul PK960. VWR Universal 200UL Pipet Tip, Low Retention, Pre-Sterile, 96 Tips/refill, 10 refills/Pack, 5 Packs/Case, 4800 Tips/Case, Certified-Free Dnase, Rnase, and human DNA, Color: Clear, Size: 300ul
        # Capture:96 Tips/refill, 10 refills/Pack, 5 Packs/Case, 4800 Tips/Case
        self.pat_for_uoi_5 = '[ \(]({0}\s?{1}/{2})'.format(self.pat_for_decimal, self.pat_for_uom_all_words, self.pat_for_uom_word)
        # Silicone gasket, 10x14.9 mm (10 pieces)
        # Capture: 10 pieces
        self.pat_for_uoi_6 = '[ \(]({0}[/= ]{1})'.format(self.pat_for_decimal, self.pat_for_uom_all_words)
        # DISPOSABLE RESPIRATOR STORAGE BAG 96 EA/box
        # Capture: 96 EA/box
        self.pat_for_uoi_7 = '[ \(]({0}\s?{1}/{2})'.format(self.pat_for_decimal, self.pat_for_uom, self.pat_for_uom_word)
        # SUPERFROST SLIDE PM MICRO WHT 1PK=1/2GR
        # Capture: 1PK=1/2GR
        self.pat_for_uoi_8 = '[ \(]({0}\s?{1}\={0}/{0}\s?{1})'.format(self.pat_for_decimal, self.pat_for_uom)


        self.pat_d_length_unit = '([cmnuμ]?m|[Ii][Nn]\.?|inch(es)?|ft\.?|feet)'
        self.pat_f_length_unit = '([Ii][Nn]\.?|inch(es)?)'

        self.pat_for_length = '(?<=\W)({0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_d_length_unit)
        self.pat_for_length_range = '(?<=\W)({0}{1}\s?(\-|to){0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_d_length_unit)

        self.pat_for_weight_unit = '([kmuμ]?g|lbs\.?|pounds)'
        self.pat_for_weight = '(?<=\W)({0}{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_weight_unit)
        self.pat_for_weight_range = '(?<=\W)({0}{1}\s?(\-|to)\s?{0}{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_weight_unit)

        self.pat_for_volume_unit = '(gallons?|gal\.?|[kmuμ]?[lL]|cu\.?\s?ft\.?)'
        self.pat_for_volume = '(?<=\W)({0}{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_volume_unit)
        self.pat_for_volume_range = '(?<=\W)({0}{1}\s?(\-|to)\s?{0}{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_volume_unit)


        self.pat_d_dimension_step = '{0}\s?((W|D|H|L)?\s?{1}?)'.format(self.pat_for_decimal, self.pat_d_length_unit)
        self.pat_d_dimension = '(?<=[\W^\.])((([Ii]nterior|[Ii]nner|[Ee]xterior)?:?\s?[Dd]imensions?\:?)?{0}?\s?[xX]\s?{0}?\s?[xX]\s?{1}\s?(W|D|H)?\s?{2})(?=\W)'.format(self.pat_d_dimension_step, self.pat_for_decimal, self.pat_d_length_unit)
        self.pat_d2_dimension = '(?<=[\W^\.])((([Ii]nterior|[Ii]nner|[Ee]xterior)?:?\s?[Dd]imensions?\:?)?{0}?\s?[xX]\s?{1}\s?(W|D|H)?\s?{2})(?=\W)'.format(self.pat_d_dimension_step, self.pat_for_decimal, self.pat_d_length_unit)

        self.pat_f_dimension_step = '({0}\s?((W|D|H|L)?\s?{1}?)?)'.format(self.pat_for_fractions, self.pat_f_length_unit)
        self.pat_f_dimension = '(?<=[\W^\.])((([Ii]nterior|[Ii]nner|[Ee]xterior)?:?\s?[Dd]imensions?\:?)?{0}?\s?[xX]\s?{0}?\s?[xX]\s?{1}\s?(W|D|H)?\s?{2})(?=\W)'.format(self.pat_f_dimension_step, self.pat_for_decimal, self.pat_f_length_unit)
        self.pat_f2_dimension = '(?<=[\W^\.])((([Ii]nterior|[Ii]nner|[Ee]xterior)?:?\s?[Dd]imensions?\:?)?{0}?\s?[xX]\s?{1}\s?(W|D|H)?\s?{2})(?=\W)'.format(self.pat_f_dimension_step, self.pat_for_decimal, self.pat_f_length_unit)


        self.pat_for_temp_unit = '°[cCfF]'
        self.pat_for_ambient = '([aA]mbient|[aA]mb\.?)'

        self.pat_for_voltage_unit = '[Mk]?VA?C?'
        self.pat_for_voltage = '(?<=\W)(([Vv]oltage:?\s?)?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_voltage_unit)
        self.pat_for_voltage_range = '(?<=\W)(([Vv]oltage\s?)?([Rr]ange:?\s?)?{0}({1})?\s?(\-|to)\s?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_voltage_unit)
        self.pat_for_voltage_tolerance = '(?<=\W)(([Vv]oltage:?\s?)?{0}{1}\s?/\s?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_voltage_unit)

        self.pat_for_wattage_unit = '[Mmkuμ][Ww]'
        self.pat_for_wattage = '(?<=\W)({0}\s?{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_wattage_unit)
        self.pat_for_wattage_range = '(?<=\W)({0}({1})?\s?(\-|to)\s?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_wattage_unit)
        self.pat_for_wattage_tolerance = '(?<=\W)({0}({1})?\s?/\s?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_wattage_unit)

        self.pat_for_frequency_unit = '[GMTk]H[zZ]{1,2}'
        self.pat_for_frequency = '(?<=\W)({0}\s?{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_frequency_unit)
        self.pat_for_frequency_range = '(?<=\W)({0}({1})?\s?(\-|to)\s?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_frequency_unit)
        self.pat_for_frequency_tolerance = '(?<=\W)({0}({1})?\s?/\s?{0}{1})(?=\W)'.format(self.pat_for_decimal, self.pat_for_frequency_unit)

        self.pat_for_electrical = '(?<=\W)((([Vv]oltage\s?)?([Rr]ange:?\s?)?{0}({1})?\s?(\-|to)\s?{0}{1})[/,]\s?{0}\s?({2})?)(?=\W)(?![-/])'.format(self.pat_for_decimal, self.pat_for_voltage_unit, '[GMTk]H[zZ]{1,2}')

        self.pat_for_time_unit = '(milli)?(second\s?|sec\.?|minutes?|min\.?|hour\s?|hr\.?|ms)'
        self.pat_for_rate = '(?<=\W)({0}\s?[a-zA-Z]{1}\s?/\s?{2})(?=\W)'.format(self.pat_for_decimal, '{1,4}', self.pat_for_time_unit)
        self.pat_for_rate_range = '(?<=\W)({0}\s?(\-|to)\s?{0}\s?[a-zA-Z]{1}\s?/\s?{2})(?=\W)'.format(self.pat_for_decimal, '{1,4}', self.pat_for_time_unit)
        self.pat_for_rpm = '(?<=\W)(({0}\s?(\-|to)\s?)?{0}\s?rpm)(?=\W)'.format(self.pat_for_decimal)


        self.pat_for_concentration = '(?<=\W)({0}\s?{1}/{2})(?=\W)'.format(self.pat_for_decimal, self.pat_for_weight_unit, self.pat_for_volume_unit)

        self.pat_for_pressure_unit = 'mbar|psi|pascal|pa'
        self.pat_for_pressure = '(?<=\W)({0}\s?({1}))(?=\W)'.format(self.pat_for_decimal,self.pat_for_pressure_unit)
        self.pat_for_pressure_range = '(?<=\W)({0}({1})?\s?(\-|to)\s?{0}{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_pressure_unit)

        self.pat_for_ppm = '(?<=\W)({0}\s?pp[mbtq])(?=\W)'.format(self.pat_for_decimal)

        self.pat_for_color = '(?<=\W)((dark|flat|light|medium|pale)?\s?(amber|black|blue|bronze|brown|clear|colorless|gold|gray|grey|green|natural|neutral|orange|pink|purple|red|silver|tan|turquoise|white|yellow))(?=\W)'

        # to be done
        self.pat_for_diameter = '5 Âµm'.format()
        self.pat_for_inner_diameter = '(1.09 mm I.D.)'.format()
        self.pat_for_outer_diameter = '(1.09 mm O.D.)'.format()


        # this one is as example
        # self.pat_for_uoi = '(\d{1,5}/pk)'.format()

        self.pat_for_thickness_unit = '((microns?)|cm|mm|mil|gauge|ga\.|inch(es)?|[Ii][Nn]\.?|ft\.?|feet|m)'
        self.pat_for_thickness = '\s?({0}{1})'.format(self.pat_for_fractions,self.pat_for_thickness_unit)
        self.pat_for_thickness_range = '\s?({0}{1}?\s?(\-|to){0}{1})'.format(self.pat_for_fractions,self.pat_for_thickness_unit)

    def wipe_injection(self, phrase):
        phrase = re.sub(self.pat_for_clean, '', phrase)
        phrase = self.post_scrub(phrase)
        return phrase

    def post_scrub(self, phrase):
        if (phrase[0] == ' ') or (phrase[0] == '(') or (phrase[0] == '/'):
            phrase = phrase[1:]

        count = 0
        while ',,' in phrase:
            phrase = phrase.replace(',,',',')
            count += 1
            if count > 10:
                break

        count = 0
        while '. .' in phrase:
            phrase = phrase.replace('. .','.')
            if count > 10:
                break

        count = 0
        while ', ,' in phrase:
            count += 1
            phrase = phrase.replace(', , ',', ')
            if count > 10:
                break

        count = 0
        while '  ' in phrase:
            count += 1
            phrase = phrase.replace('  ', ' ')
            if count > 10:
                break

        phrase = phrase.replace(', ;', '.')
        phrase = phrase.replace(', .', '.')
        phrase = phrase.replace(' ,', ',')
        if phrase[0] == '/':
            phrase = phrase[1:]

        return phrase

    def pre_scrub(self, phrase):
        phrase = phrase + ' '
        phrase = phrase.replace('  ',' ')
        phrase = phrase.replace('..','.')
        phrase = phrase.replace(',,',',')
        phrase = re.sub('(?<=\d)(")','in.',phrase)
        phrase = re.sub('(?<=\d)(\')','ft.',phrase)
        return phrase

    def reinject_phrase(self, phrase):
        phrase = phrase.format(*self.lst_extractions)
        self.lst_extractions = []
        return phrase

    def extract_attributes(self, outer_text, use_pattern = ''):
        attribute = ''
        # this removes a little ugly junk
        outer_text = self.pre_scrub(outer_text)
        if use_pattern != '':
            # does pattern matching
            pattern = re.compile(use_pattern, flags=re.IGNORECASE)
            lst_result = pattern.findall(outer_text)

            # this should refine the results and return a single string value
            outer_text, attribute = self.refine_results(outer_text, lst_result)

            # x = input('attribute')
        return outer_text, attribute

    def refine_results(self, outer_text, lst_result):
        cur_attribute = ''
        cur_attribute_ps = ''
        dirty_attribute = ''
        if len(lst_result) > 0:
            # lst_result is list of tuples
            # each_tup/lst_result[0] is tuple of result strings
            # first_results is list of result strings
            for each_tup in lst_result:
                if isinstance(each_tup, tuple):
                    results = list(each_tup)
                    results.sort(key=lambda x:len(x))
                    results.reverse()
                    cur_attribute = results[0]
                else:
                    cur_attribute = each_tup

                cur_attribute_ps = self.post_scrub(cur_attribute)

                if (cur_attribute_ps not in dirty_attribute) and (cur_attribute_ps != ''):
                    outer_text = outer_text.replace(cur_attribute_ps, '{'+str(len(self.lst_extractions))+'}')
                    self.lst_extractions.append(cur_attribute_ps)
                    if dirty_attribute != '':
                        dirty_attribute = dirty_attribute+'; '+cur_attribute_ps
                    else:
                        dirty_attribute = cur_attribute_ps

        return outer_text, dirty_attribute

    def extract_dimensions(self, container_str):
        dimensions = ''
        # this will extract stupid american style numbers like '4 5/16"'
        container_str, dimensions = self.extract_attributes(container_str, self.pat_f_dimension)
        dimensions = re.sub('(\s?[xX]\s?)',' X ',dimensions)

        if dimensions == '':
            # if the first doesn't extract we try it again with decimals
            container_str, dimensions = self.extract_attributes(container_str, self.pat_d_dimension)
            dimensions = re.sub('(\s?[xX]\s?)',' X ',dimensions)
        else:
            return container_str, dimensions

        if dimensions == '':
            container_str, dimensions = self.extract_attributes(container_str, self.pat_d2_dimension)
            dimensions = re.sub('(\s?[xX]\s?)',' X ',dimensions)
        else:
            return container_str, dimensions

        if dimensions == '':
            container_str, dimensions = self.extract_attributes(container_str, self.pat_f2_dimension)
            dimensions = re.sub('(\s?[xX]\s?)',' X ',dimensions)

        return container_str, dimensions

    def temp_prep(self,container_str):
        container_str = re.sub('(?<=[\d\s\(])(\[?[dD]egrees?\]?|[dD]eg\.?)(?=[cCfF]?\W)','°',container_str)
        container_str = re.sub('(?<=\W)([pP]lus)(?=\d)', '+', container_str)
        container_str = re.sub('(?<=\d)(°\sto)(?=\s\+?\d)', ' to', container_str)

        # container_str = re.sub('(?<=\W)([aA]mbient|[aA]mb\.?)(?=\W)', '20°', container_str)

        return container_str

    def extract_temp_range(self,container_str):
        self.pat_d_temp_range = '(?<=\W)(\+?-?{0}\s?({1})?(\s?\-\s?|\s?to\s?)\+?-?{0}\s?{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_temp_unit)
        container_str = self.temp_prep(container_str)

        temperature_range = ''
        container_str, temperature_range = self.extract_attributes(container_str, self.pat_d_temp_range)

        if temperature_range == '':
            ex = ['5° above ambient','Ambient +392deg.F (+200deg.C)','Ambient to +100deg.C (+212deg.F);','Ambient to 65°C.','Temperature Range 10 below ambient to65°C.','Temperature Range 5 above ambient to 65°C','Ambient to 65°C']
            self.pat_d_temp_range = '(?<=\W\s)({0}(\s?\-\s?|\s?to\s?)\+?-?{1}\s?{2})(?=\W)'.format(self.pat_for_ambient,self.pat_for_decimal,self.pat_for_temp_unit)
            container_str, temperature_range = self.extract_attributes(container_str, self.pat_d_temp_range)

        if temperature_range != '':
            temperature_range = re.sub('(?<=\d)(\s?to)(?=\d)', ' to ', temperature_range)
            temperature_range = re.sub('(?<=\d)(\s?-)(?=\d)', ' to ', temperature_range)

            if temperature_range[0] != '-':
                temperature_range = re.sub('(?<=\d)(\sto\s?\+)(?=\d)',' to ',temperature_range)

            if temperature_range[0] == '+':
                temperature_range = temperature_range[1:]

        return container_str, temperature_range

    def extract_temperature(self,container_str):
        self.pat_d_temp = '(?<=\W)(\+?-?{0}\s?{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_temp_unit)
        self.pat_d_temp_tolerance = '(?<=\W)(\+/?-{0}\s?{1})(?=\W)'.format(self.pat_for_decimal,self.pat_for_temp_unit)

        container_str = self.temp_prep(container_str)

        temperature_tolerance = ''
        container_str, temperature_tolerance = self.extract_attributes(container_str, self.pat_d_temp_tolerance)

        temperature = ''
        container_str, temperature = self.extract_attributes(container_str, self.pat_d_temp)

        if temperature != '' and temperature_tolerance != '':
            temperature = temperature + '; ' + temperature_tolerance
        elif temperature == '':
            temperature = temperature_tolerance

        return container_str, temperature

    def extract_voltage_range(self,container_str):
        voltage_range = ''
        container_str, voltage_range = self.extract_attributes(container_str, self.pat_for_voltage_range)

        return container_str, voltage_range

    def extract_voltage(self,container_str):
        voltage_tolerance = ''
        container_str, voltage_tolerance = self.extract_attributes(container_str, self.pat_for_voltage_tolerance)

        voltage = ''
        container_str, voltage = self.extract_attributes(container_str, self.pat_for_voltage)

        if voltage != '' and voltage_tolerance != '':
            voltage = voltage + '; ' + voltage_tolerance
        elif voltage == '':
            voltage = voltage_tolerance

        return container_str, voltage

    def extract_wattage(self,container_str):
        wattage = ''
        container_str, wattage = self.extract_attributes(container_str,self.pat_for_wattage)
        return container_str, wattage

    def extract_concentration(self,container_str):
        concentration = ''
        container_str, concentration = self.extract_attributes(container_str, self.pat_for_concentration)
        return container_str, concentration

    def extract_rate(self,container_str):
        rate = ''
        container_str, rate = self.extract_attributes(container_str, self.pat_for_rate)
        rate_range = ''
        container_str, rate_range = self.extract_attributes(container_str, self.pat_for_rate_range)
        if (rate != '') and (rate_range != ''):
            rate = rate + '; ' + rate_range
        elif rate == '':
            rate = rate_range

        rpm = ''
        container_str, rpm = self.extract_attributes(container_str, self.pat_for_rpm)
        if (rate != '') and (rpm != ''):
            rate = rate + '; ' + rpm
        elif rate == '':
            rate = rpm

        return container_str, rate

    def extract_frequency(self,container_str):
        frequency = ''
        frequency_tolerance = ''
        container_str, frequency_tolerance = self.extract_attributes(container_str, self.pat_for_frequency_tolerance)

        frequency = ''
        container_str, frequency = self.extract_attributes(container_str, self.pat_for_frequency)

        if frequency != '' and frequency_tolerance != '':
            frequency = frequency + '; ' + frequency_tolerance
        elif frequency == '':
            frequency = frequency_tolerance

        return container_str, frequency

    def extract_pressure(self,container_str):
        pressure = ''
        pressure_range = ''
        container_str, pressure = self.extract_attributes(container_str, self.pat_for_pressure)
        container_str, pressure_range = self.extract_attributes(container_str, self.pat_for_pressure_range)
        if pressure_range != '':
            if pressure != '':
                pressure = pressure +'; '+ pressure_range
            else:
                pressure = pressure_range

        return container_str, pressure

    def extract_ppm(self,container_str):
        ppm = ''
        container_str, ppm = self.extract_attributes(container_str, self.pat_for_ppm)
        return container_str, ppm

    def extract_color(self,container_str):
        color = ''
        container_str, color = self.extract_attributes(container_str, self.pat_for_color)
        return container_str, color

    def extract_electrical(self,container_str):
        electrical = ''
        container_str, electrical = self.extract_attributes(container_str, self.pat_for_electrical)
        return container_str, electrical


    def extract_uoi(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi)
        return container_str, uoi

    def extract_uoi_1(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_1)
        return container_str, uoi

    def extract_uoi_2(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_2)
        return container_str, uoi

    def extract_uoi_3(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_3)
        return container_str, uoi

    def extract_uoi_4(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_4)
        return container_str, uoi

    def extract_uoi_5(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_5)
        return container_str, uoi

    def extract_uoi_6(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_6)
        return container_str, uoi

    def extract_uoi_7(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_7)
        return container_str, uoi

    def extract_uoi_8(self, container_str):
        uoi = ''
        container_str, uoi = self.extract_attributes(container_str, self.pat_for_uoi_8)
        return container_str, uoi



def test_frame():
    obExtractor = Extractor()
    in_string = 'LAB COAT WHITE BUTTON MENS XL 1EA/25CS'
    out_string, terms = obExtractor.extract_uoi_3(in_string)


    print(terms)
    print(out_string)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    test_frame()

## end ##