#!/usr/bin/env python3
"""
Скрипт для извлечения полного списка брендов из HTML структуры drom.ru
"""
import json
from ...src.auto_reviews_parser.utils.brand_extractor import BrandExtractor


def extract_brands_from_provided_html():
    """Извлекаем бренды из предоставленного пользователем HTML"""

    # HTML контент предоставленный пользователем
    html_content = """
    <div class="_1ggdsc70 _1ggdsc71 css-10ib5jr" data-ftid="component_cars-list">
    <noscript>
    <a href="https://www.drom.ru/reviews/ac/">AC</a>
    <a href="https://www.drom.ru/reviews/aito/">AITO</a>
    <a href="https://www.drom.ru/reviews/acura/">Acura</a>
    <a href="https://www.drom.ru/reviews/alfa_romeo/">Alfa Romeo</a>
    <a href="https://www.drom.ru/reviews/alpina/">Alpina</a>
    <a href="https://www.drom.ru/reviews/alpine/">Alpine</a>
    <a href="https://www.drom.ru/reviews/arcfox/">Arcfox</a>
    <a href="https://www.drom.ru/reviews/aro/">Aro</a>
    <a href="https://www.drom.ru/reviews/asia/">Asia</a>
    <a href="https://www.drom.ru/reviews/aston_martin/">Aston Martin</a>
    <a href="https://www.drom.ru/reviews/audi/">Audi</a>
    <a href="https://www.drom.ru/reviews/avatr/">Avatr</a>
    <a href="https://www.drom.ru/reviews/baic/">BAIC</a>
    <a href="https://www.drom.ru/reviews/baw/">BAW</a>
    <a href="https://www.drom.ru/reviews/bmw/">BMW</a>
    <a href="https://www.drom.ru/reviews/byd/">BYD</a>
    <a href="https://www.drom.ru/reviews/baojun/">Baojun</a>
    <a href="https://www.drom.ru/reviews/belgee/">Belgee</a>
    <a href="https://www.drom.ru/reviews/bentley/">Bentley</a>
    <a href="https://www.drom.ru/reviews/brilliance/">Brilliance</a>
    <a href="https://www.drom.ru/reviews/bugatti/">Bugatti</a>
    <a href="https://www.drom.ru/reviews/buick/">Buick</a>
    <a href="https://www.drom.ru/reviews/cadillac/">Cadillac</a>
    <a href="https://www.drom.ru/reviews/changan/">Changan</a>
    <a href="https://www.drom.ru/reviews/changhe/">Changhe</a>
    <a href="https://www.drom.ru/reviews/chery/">Chery</a>
    <a href="https://www.drom.ru/reviews/chevrolet/">Chevrolet</a>
    <a href="https://www.drom.ru/reviews/chrysler/">Chrysler</a>
    <a href="https://www.drom.ru/reviews/ciimo/">Ciimo</a>
    <a href="https://www.drom.ru/reviews/citroen/">Citroen</a>
    <a href="https://www.drom.ru/reviews/dw_hower/">DW Hower</a>
    <a href="https://www.drom.ru/reviews/dacia/">Dacia</a>
    <a href="https://www.drom.ru/reviews/dadi/">Dadi</a>
    <a href="https://www.drom.ru/reviews/daewoo/">Daewoo</a>
    <a href="https://www.drom.ru/reviews/daihatsu/">Daihatsu</a>
    <a href="https://www.drom.ru/reviews/daimler/">Daimler</a>
    <a href="https://www.drom.ru/reviews/datsun/">Datsun</a>
    <a href="https://www.drom.ru/reviews/dayun/">Dayun</a>
    <a href="https://www.drom.ru/reviews/denza/">Denza</a>
    <a href="https://www.drom.ru/reviews/derways/">Derways</a>
    <a href="https://www.drom.ru/reviews/dodge/">Dodge</a>
    <a href="https://www.drom.ru/reviews/dongfeng/">Dongfeng</a>
    <a href="https://www.drom.ru/reviews/cheryexeed/">EXEED</a>
    <a href="https://www.drom.ru/reviews/eagle/">Eagle</a>
    <a href="https://www.drom.ru/reviews/faw/">FAW</a>
    <a href="https://www.drom.ru/reviews/ferrari/">Ferrari</a>
    <a href="https://www.drom.ru/reviews/fiat/">Fiat</a>
    <a href="https://www.drom.ru/reviews/ford/">Ford</a>
    <a href="https://www.drom.ru/reviews/forthing/">Forthing</a>
    <a href="https://www.drom.ru/reviews/foton/">Foton</a>
    <a href="https://www.drom.ru/reviews/freightliner/">Freightliner</a>
    <a href="https://www.drom.ru/reviews/gac/">GAC</a>
    <a href="https://www.drom.ru/reviews/gmc/">GMC</a>
    <a href="https://www.drom.ru/reviews/geely/">Geely</a>
    <a href="https://www.drom.ru/reviews/genesis/">Genesis</a>
    <a href="https://www.drom.ru/reviews/geo/">Geo</a>
    <a href="https://www.drom.ru/reviews/great_wall/">Great Wall</a>
    <a href="https://www.drom.ru/reviews/hafei/">Hafei</a>
    <a href="https://www.drom.ru/reviews/haima/">Haima</a>
    <a href="https://www.drom.ru/reviews/haval/">Haval</a>
    <a href="https://www.drom.ru/reviews/hawtai/">Hawtai</a>
    <a href="https://www.drom.ru/reviews/hiphi/">HiPhi</a>
    <a href="https://www.drom.ru/reviews/higer/">Higer</a>
    <a href="https://www.drom.ru/reviews/hino/">Hino</a>
    <a href="https://www.drom.ru/reviews/honda/">Honda</a>
    <a href="https://www.drom.ru/reviews/hongqi/">Hongqi</a>
    <a href="https://www.drom.ru/reviews/howo/">Howo</a>
    <a href="https://www.drom.ru/reviews/hozon/">Hozon</a>
    <a href="https://www.drom.ru/reviews/huanghai/">Huanghai</a>
    <a href="https://www.drom.ru/reviews/hummer/">Hummer</a>
    <a href="https://www.drom.ru/reviews/hyundai/">Hyundai</a>
    <a href="https://www.drom.ru/reviews/im_motors/">IM Motors</a>
    <a href="https://www.drom.ru/reviews/iveco/">IVECO</a>
    <a href="https://www.drom.ru/reviews/infiniti/">Infiniti</a>
    <a href="https://www.drom.ru/reviews/iran_khodro/">Iran Khodro</a>
    <a href="https://www.drom.ru/reviews/isuzu/">Isuzu</a>
    <a href="https://www.drom.ru/reviews/jac/">JAC</a>
    <a href="https://www.drom.ru/reviews/jmc/">JMC</a>
    <a href="https://www.drom.ru/reviews/jmev/">JMEV</a>
    <a href="https://www.drom.ru/reviews/jaecoo/">Jaecoo</a>
    <a href="https://www.drom.ru/reviews/jaguar/">Jaguar</a>
    <a href="https://www.drom.ru/reviews/jeep/">Jeep</a>
    <a href="https://www.drom.ru/reviews/jetour/">Jetour</a>
    <a href="https://www.drom.ru/reviews/jetta/">Jetta</a>
    <a href="https://www.drom.ru/reviews/jidu/">Jidu</a>
    <a href="https://www.drom.ru/reviews/kaiyi/">Kaiyi</a>
    <a href="https://www.drom.ru/reviews/kia/">Kia</a>
    <a href="https://www.drom.ru/reviews/knewstar/">Knewstar</a>
    <a href="https://www.drom.ru/reviews/koenigsegg/">Koenigsegg</a>
    <a href="https://www.drom.ru/reviews/kuayue/">Kuayue</a>
    <a href="https://www.drom.ru/reviews/lamborghini/">Lamborghini</a>
    <a href="https://www.drom.ru/reviews/lancia/">Lancia</a>
    <a href="https://www.drom.ru/reviews/land_rover/">Land Rover</a>
    <a href="https://www.drom.ru/reviews/leapmotor/">Leapmotor</a>
    <a href="https://www.drom.ru/reviews/lexus/">Lexus</a>
    <a href="https://www.drom.ru/reviews/li/">Li</a>
    <a href="https://www.drom.ru/reviews/lifan/">Lifan</a>
    <a href="https://www.drom.ru/reviews/lincoln/">Lincoln</a>
    <a href="https://www.drom.ru/reviews/livan/">Livan</a>
    <a href="https://www.drom.ru/reviews/lotus/">Lotus</a>
    <a href="https://www.drom.ru/reviews/luxeed/">Luxeed</a>
    <a href="https://www.drom.ru/reviews/luxgen/">Luxgen</a>
    <a href="https://www.drom.ru/reviews/lynk_and_co/">Lynk &amp; Co</a>
    <a href="https://www.drom.ru/reviews/m-hero/">M-Hero</a>
    <a href="https://www.drom.ru/reviews/mg/">MG</a>
    <a href="https://www.drom.ru/reviews/mini/">MINI</a>
    <a href="https://www.drom.ru/reviews/marussia/">Marussia</a>
    <a href="https://www.drom.ru/reviews/maserati/">Maserati</a>
    <a href="https://www.drom.ru/reviews/maxus/">Maxus</a>
    <a href="https://www.drom.ru/reviews/maybach/">Maybach</a>
    <a href="https://www.drom.ru/reviews/mazda/">Mazda</a>
    <a href="https://www.drom.ru/reviews/mclaren/">McLaren</a>
    <a href="https://www.drom.ru/reviews/mercedes-benz/">Mercedes-Benz</a>
    <a href="https://www.drom.ru/reviews/mercury/">Mercury</a>
    <a href="https://www.drom.ru/reviews/mitsubishi/">Mitsubishi</a>
    <a href="https://www.drom.ru/reviews/mitsuoka/">Mitsuoka</a>
    <a href="https://www.drom.ru/reviews/nio/">Nio</a>
    <a href="https://www.drom.ru/reviews/nissan/">Nissan</a>
    <a href="https://www.drom.ru/reviews/omoda/">OMODA</a>
    <a href="https://www.drom.ru/reviews/ora/">ORA</a>
    <a href="https://www.drom.ru/reviews/oldsmobile/">Oldsmobile</a>
    <a href="https://www.drom.ru/reviews/opel/">Opel</a>
    <a href="https://www.drom.ru/reviews/oshan/">Oshan</a>
    <a href="https://www.drom.ru/reviews/oting/">Oting</a>
    <a href="https://www.drom.ru/reviews/peugeot/">Peugeot</a>
    <a href="https://www.drom.ru/reviews/plymouth/">Plymouth</a>
    <a href="https://www.drom.ru/reviews/polar_stone/">Polar Stone</a>
    <a href="https://www.drom.ru/reviews/polestar/">Polestar</a>
    <a href="https://www.drom.ru/reviews/pontiac/">Pontiac</a>
    <a href="https://www.drom.ru/reviews/porsche/">Porsche</a>
    <a href="https://www.drom.ru/reviews/proton/">Proton</a>
    <a href="https://www.drom.ru/reviews/ram/">RAM</a>
    <a href="https://www.drom.ru/reviews/radar/">Radar</a>
    <a href="https://www.drom.ru/reviews/ravon/">Ravon</a>
    <a href="https://www.drom.ru/reviews/renault/">Renault</a>
    <a href="https://www.drom.ru/reviews/renault_samsung/">Renault Samsung</a>
    <a href="https://www.drom.ru/reviews/rising_auto/">Rising Auto</a>
    <a href="https://www.drom.ru/reviews/rivian/">Rivian</a>
    <a href="https://www.drom.ru/reviews/roewe/">Roewe</a>
    <a href="https://www.drom.ru/reviews/rolls-royce/">Rolls-Royce</a>
    <a href="https://www.drom.ru/reviews/rover/">Rover</a>
    <a href="https://www.drom.ru/reviews/rox/">Rox</a>
    <a href="https://www.drom.ru/reviews/seat/">SEAT</a>
    <a href="https://www.drom.ru/reviews/swm/">SWM</a>
    <a href="https://www.drom.ru/reviews/saab/">Saab</a>
    <a href="https://www.drom.ru/reviews/saturn/">Saturn</a>
    <a href="https://www.drom.ru/reviews/scania/">Scania</a>
    <a href="https://www.drom.ru/reviews/scion/">Scion</a>
    <a href="https://www.drom.ru/reviews/seres/">Seres</a>
    <a href="https://www.drom.ru/reviews/shuanghuan/">Shuanghuan</a>
    <a href="https://www.drom.ru/reviews/skoda/">Skoda</a>
    <a href="https://www.drom.ru/reviews/skywell/">Skywell</a>
    <a href="https://www.drom.ru/reviews/smart/">Smart</a>
    <a href="https://www.drom.ru/reviews/solaris-agr/">Solaris</a>
    <a href="https://www.drom.ru/reviews/soueast/">Soueast</a>
    <a href="https://www.drom.ru/reviews/ssang_yong/">SsangYong</a>
    <a href="https://www.drom.ru/reviews/subaru/">Subaru</a>
    <a href="https://www.drom.ru/reviews/suzuki/">Suzuki</a>
    <a href="https://www.drom.ru/reviews/tata/">TATA</a>
    <a href="https://www.drom.ru/reviews/tank/">Tank</a>
    <a href="https://www.drom.ru/reviews/tesla/">Tesla</a>
    <a href="https://www.drom.ru/reviews/tianma/">Tianma</a>
    <a href="https://www.drom.ru/reviews/tianye/">Tianye</a>
    <a href="https://www.drom.ru/reviews/toyota/">Toyota</a>
    <a href="https://www.drom.ru/reviews/trabant/">Trabant</a>
    <a href="https://www.drom.ru/reviews/vgv/">VGV</a>
    <a href="https://www.drom.ru/reviews/venucia/">Venucia</a>
    <a href="https://www.drom.ru/reviews/volkswagen/">Volkswagen</a>
    <a href="https://www.drom.ru/reviews/volvo/">Volvo</a>
    <a href="https://www.drom.ru/reviews/vortex/">Vortex</a>
    <a href="https://www.drom.ru/reviews/voyah/">Voyah</a>
    <a href="https://www.drom.ru/reviews/wey/">WEY</a>
    <a href="https://www.drom.ru/reviews/wartburg/">Wartburg</a>
    <a href="https://www.drom.ru/reviews/weltmeister/">Weltmeister</a>
    <a href="https://www.drom.ru/reviews/wuling/">Wuling</a>
    <a href="https://www.drom.ru/reviews/xcite/">Xcite</a>
    <a href="https://www.drom.ru/reviews/xiaomi/">Xiaomi</a>
    <a href="https://www.drom.ru/reviews/xin_kai/">Xin Kai</a>
    <a href="https://www.drom.ru/reviews/xpeng/">Xpeng</a>
    <a href="https://www.drom.ru/reviews/zx/">ZX</a>
    <a href="https://www.drom.ru/reviews/zeekr/">Zeekr</a>
    <a href="https://www.drom.ru/reviews/zotye/">Zotye</a>
    <a href="https://www.drom.ru/reviews/icar/">iCAR</a>
    <a href="https://www.drom.ru/reviews/amber/">Амберавто</a>
    <a href="https://www.drom.ru/reviews/aurus/">Аурус</a>
    <a href="https://www.drom.ru/reviews/bogdan/">Богдан</a>
    <a href="https://www.drom.ru/reviews/gaz/">ГАЗ</a>
    <a href="https://www.drom.ru/reviews/doninvest/">Донинвест</a>
    <a href="https://www.drom.ru/reviews/zaz/">ЗАЗ</a>
    <a href="https://www.drom.ru/reviews/zil/">ЗИЛ</a>
    <a href="https://www.drom.ru/reviews/zis/">ЗиС</a>
    <a href="https://www.drom.ru/reviews/izh/">ИЖ</a>
    <a href="https://www.drom.ru/reviews/kamaz/">КамАЗ</a>
    <a href="https://www.drom.ru/reviews/lada/">Лада</a>
    <a href="https://www.drom.ru/reviews/luaz/">ЛуАЗ</a>
    <a href="https://www.drom.ru/reviews/moskvitch/">Москвич</a>
    <a href="https://www.drom.ru/reviews/other/">Прочие авто</a>
    <a href="https://www.drom.ru/reviews/raf/">РАФ</a>
    <a href="https://www.drom.ru/reviews/sollers/">Соллерс</a>
    <a href="https://www.drom.ru/reviews/tagaz/">ТагАЗ</a>
    <a href="https://www.drom.ru/reviews/uaz/">УАЗ</a>
    <a href="https://www.drom.ru/reviews/evolute/">Эволют</a>
    </noscript>
    </div>
    """

    print("Извлекаем бренды из предоставленного HTML...")
    brands = BrandExtractor.extract_brands_from_noscript(html_content)

    print(f"Найдено брендов: {len(brands)}")

    # Сохраняем результат
    output_file = "../../data/exports/full_brands_catalog.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(brands, f, ensure_ascii=False, indent=2)

    print(f"Результат сохранен в: {output_file}")

    # Показываем первые 10 брендов для проверки
    print("\nПервые 10 брендов:")
    for i, brand in enumerate(brands[:10]):
        print(f"{i+1}. {brand['name']} ({brand['slug']}) - {brand['url']}")

    # Статистика
    print(f"\nВсего брендов: {len(brands)}")
    russian_brands = len([b for b in brands if any(ord(c) > 127 for c in b["name"])])
    latin_brands = len([b for b in brands if all(ord(c) <= 127 for c in b["name"])])
    print(f"Русские бренды: {russian_brands}")
    print(f"Латинские бренды: {latin_brands}")

    return brands


if __name__ == "__main__":
    brands = extract_brands_from_provided_html()
