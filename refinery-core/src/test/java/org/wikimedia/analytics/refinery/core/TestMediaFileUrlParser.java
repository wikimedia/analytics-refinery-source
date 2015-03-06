// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.core;

import junit.framework.TestCase;

public class TestMediaFileUrlParser extends TestCase {

    // Helper methods ---------------------------------------------------------

    private void assertParsed(final String url,
            final MediaFileUrlInfo expected) {
        MediaFileUrlInfo actual = MediaFileUrlParser.parse(url);

        assertEquals("Parsed info does not equal expected", expected, actual);
    }

    private void assertUnidentified(final String url) {
        assertParsed(url, null);
    }

    private void assertOriginal(final String url,
            final String baseName) {
        assertParsed(url, MediaFileUrlInfo.createOriginal(baseName));
    }

    private void assertOriginal(final String url) {
        assertOriginal(url, url);
    }

    private void assertImage(final String url,
            final String baseName, final Integer width) {
        assertParsed(url,
                MediaFileUrlInfo.createTranscodedToImage(baseName, width));
    }

    private void assertMovie(final String url,
            final String baseName, final int height) {
        assertParsed(url,
                MediaFileUrlInfo.createTranscodedToMovie(baseName, height));
    }

    private void assertAudio(final String url, final String baseName) {
        assertParsed(url, MediaFileUrlInfo.createTranscodedToAudio(baseName));
    }

    // Test degenerate settings -----------------------------------------------

    public void testNull() {
        assertUnidentified(null);
    }

    public void testEmpty() {
        assertUnidentified("");
    }

    public void testPlainSlash() {
        assertUnidentified("/");
    }

    public void testLongPixelStringLowResolution() {
        assertImage(
                "/wikipedia/commons/thumb/8/83/Kit_body.svg/00000000000000000000000000000000000000000000000000000000000000000000000000000000000000001px-Kit_body.svg.png",
                "/wikipedia/commons/8/83/Kit_body.svg", 1);
    }

    public void testLongPixelStringHighResolution() {
        assertImage(
                "/wikipedia/commons/thumb/8/83/Kit_body.svg/10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000px-Kit_body.svg.png",
                "/wikipedia/commons/8/83/Kit_body.svg",
                Integer.MAX_VALUE);
    }

    // Test protocols ---------------------------------------------------------

    public void testNoProtocolNoLeadingSlash() {
        assertUnidentified("math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testNoProtocolLeadingSlash() {
        assertOriginal("/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testHttp() {
        assertOriginal(
                "http://upload.wikimedia.org/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png",
                "/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testHttpPlainSlash() {
        assertUnidentified("http://upload.wikimedia.org/");
    }

    public void testHttps() {
        assertOriginal(
                "https://upload.wikimedia.org/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png",
                "/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testHttpsPlainSlash() {
        assertUnidentified("https://upload.wikimedia.org/");
    }

    // Test uri cleanup -------------------------------------------------------

    public void testSlashTrimming() {
        assertOriginal(
                "/math/d/a///9//da9d325123d50dbc4e36363f2863ce3e.png",
                "/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testDecodingThumbNoneEncoded() {
        assertImage(
                "/wikipedia/commons/thumb/7/7a/Japan_on_the_globe_(claimed)_(Japan_centered).svg/240px-Japan_on_the_globe_(claimed)_(Japan_centered).svg.png",
                "/wikipedia/commons/7/7a/Japan_on_the_globe_(claimed)_(Japan_centered).svg",
                240);
    }

    public void testDecodingThumbBothEncoded() {
        assertImage(
                "/wikipedia/commons/thumb/7/7a/Japan_on_the_globe_%28claimed%29_%28Japan_centered%29.svg/240px-Japan_on_the_globe_%28claimed%29_%28Japan_centered%29.svg.png",
                "/wikipedia/commons/7/7a/Japan_on_the_globe_(claimed)_(Japan_centered).svg",
                240);
    }

    public void testDecodingThumbOnlyMainPartEncoded() {
        assertImage(
                "/wikipedia/commons/thumb/7/7a/Japan_on_the_globe_%28claimed%29_%28Japan_centered%29.svg/240px-Japan_on_the_globe_(claimed)_(Japan_centered).svg.png",
                "/wikipedia/commons/7/7a/Japan_on_the_globe_(claimed)_(Japan_centered).svg",
                240);
    }

    public void testDecodingThumbOnlyThumbEncoded() {
        assertImage(
                "/wikipedia/commons/thumb/7/7a/Japan_on_the_globe_(claimed)_(Japan_centered).svg/240px-Japan_on_the_globe_%28claimed%29_%28Japan_centered%29.svg.png",
                "/wikipedia/commons/7/7a/Japan_on_the_globe_(claimed)_(Japan_centered).svg",
                240);
    }

    public void testTrimming() {
        assertImage(
                "/wikipedia/commons/thumb/a/ae/Essig-1.jpg/459px-Essig-1.jpg%20%20%20",
                "/wikipedia/commons/a/ae/Essig-1.jpg", 459);
    }

    // Test static assets -----------------------------------------------------

    public void testFavicon() {
        assertOriginal("/favicon.ico");
    }

    public void testFaviconWithPathlessSuffix() {
        assertUnidentified("/favicon.icofoo");
    }

    public void testFaviconWithPathedSuffix() {
        assertUnidentified("/favicon.ico/foo");
    }

    // Test math images -------------------------------------------------------

    public void testMathPlain() {
        assertOriginal("/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathNonHexFirstUrlPart() {
        assertUnidentified("/math/X/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathNonHexSecondUrlPart() {
        assertUnidentified("/math/d/Y/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathNonHexThirdUrlPart() {
        assertUnidentified("/math/d/a/Z/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathNonHexHash() {
        assertUnidentified("/math/d/a/9/da9d32512Qd50dbc4e36363f2863ce3e.png");
    }

    public void testMathHashTooLong() {
        assertUnidentified("/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e0.png");
    }

    public void testMathHashTooShort() {
        assertUnidentified("/math/d/a/9/da9d325123d50dbc4e36363f2863ce3.png");
    }

    public void testMathFirstHashDigitMismatch() {
        assertUnidentified("/math/0/a/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathSecondHashDigitMismatch() {
        assertUnidentified("/math/d/0/9/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathThirdHashDigitMismatch() {
        assertUnidentified("/math/d/a/0/da9d325123d50dbc4e36363f2863ce3e.png");
    }

    public void testMathPerWikiPlain() {
        assertOriginal("/wikipedia/en/math/5/6/a/56a5d0fae0136327e61476dcfe43109a.png");
    }

    // Test score -------------------------------------------------------------

    public void testScore() {
        assertImage(
                "/score/7/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png",
                "/score/7/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png", null);
    }

    public void testScoreNonAlphaNumFirstPart() {
        assertUnidentified("/score/-/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png");
    }

    public void testScoreNonAlphaNumSecondPart() {
        assertUnidentified("/score/7/-/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png");
    }

    public void testScoreNonAlphaNumThirdPart() {
        assertUnidentified("/score/7/a/7ae-9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png");
    }

    public void testScoreNonAlphaNumFourthPart() {
        assertUnidentified("/score/7/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jw-.png");
    }

    public void testScoreNonMatchingFirstPart() {
        assertUnidentified("/score/8/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png");
    }

    public void testScoreNonMatchingSecondPart() {
        assertUnidentified("/score/7/b/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png");
    }

    public void testScoreNonMatchingThirdPart() {
        assertUnidentified("/score/7/a/7aeg9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jww.png");
    }

    public void testScoreNonMatchingFourthPart() {
        assertUnidentified("/score/7/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem0jww.png");
    }

    public void testScoreTooLongThirdPart() {
        assertUnidentified("/score/7/a/7aem9jwwirkhn0ucbewj9gs7aofzc2bc/7aem9jww.png");
    }

    public void testScoreTooLongFourthPart() {
        assertUnidentified("/score/7/a/7aem9jwwirkhn0ucbewj9gs7aofzc2b/7aem9jwwi.png");
    }

    public void testScoreOgg() {
        assertAudio("/score/q/0/q0bopydzemuz315z4n6dvg8sfu8qsu0/q0bopydz.ogg",
                "/score/q/0/q0bopydzemuz315z4n6dvg8sfu8qsu0/q0bopydz.png");
    }

    public void testScoreMidi() {
        assertAudio("/score/k/7/k7yj1lvc3fqecbmknn497haqj6x9g2y/k7yj1lvc.midi",
                "/score/k/7/k7yj1lvc3fqecbmknn497haqj6x9g2y/k7yj1lvc.png");
    }

    // Test timeline image ----------------------------------------------------

    public void testTimeline() {
        assertOriginal("/wikipedia/en/timeline/12435a102adebdee9059bc97bb652af1.png");
    }

    // Test uploaded media files ----------------------------------------------

    public void testMediaMeta() {
        assertOriginal("/wikipedia/meta/7/74/Wikibooks-logo_sister_1x.png");
    }

    public void testMediaNonHexFirstPart() {
        assertUnidentified("/wikipedia/meta/X/74/Wikibooks-logo_sister_1x.png");
    }

    public void testMediaNonHexSecondPart() {
        assertUnidentified("/wikipedia/meta/7/7X/Wikibooks-logo_sister_1x.png");
    }

    public void testMediaFirstAndSecondPartMismatch() {
        assertUnidentified("/wikipedia/meta/7/84/Wikibooks-logo_sister_1x.png");
    }

    public void testMediaCommons() {
        assertOriginal("/wikipedia/commons/d/dd/Fumiyuki_Beppu_Giro_2011.jpg");
    }

    public void testMediaWikibooks() {
        assertOriginal("/wikibooks/en/b/bc/Wiki.png");
    }

    public void testMediaWiktionary() {
        assertOriginal("/wiktionary/fr/b/bc/Wiki.png");
    }

    public void testMediaWikinews() {
        assertOriginal("/wikinews/en/f/f7/Twitter.png");
    }

    public void testMediaWikiquote() {
        assertOriginal("/wikiquote/en/b/bc/Wiki.png");
    }

    public void testMediaWikisource() {
        assertOriginal("/wikisource/ar/d/dd/Foo.pdf");
    }

    public void testMediaWikiversity() {
        assertOriginal("/wikiversity/ru/b/b6/Diffuziya_v_menzurke.jpg");
    }

    public void testMediaWikivoyage() {
        assertOriginal("/wikivoyage/ru/c/ce/Map_mag.png");
    }

    public void testMediaWikimedia() {
        assertImage(
                "/wikimedia/pl/thumb/4/47/Spraw_2010_OPP.pdf/page21-180px-Spraw_2010_OPP.pdf.jpg",
                "/wikimedia/pl/4/47/Spraw_2010_OPP.pdf", 180);
    }

    public void testMediaWikimania2014() {
        assertImage(
                "/wikipedia/wikimania2014/thumb/a/ae/Rufus_Pollock.png/293px-Rufus_Pollock.png",
                "/wikipedia/wikimania2014/a/ae/Rufus_Pollock.png", 293);
    }

    // Test uploaded media files; Thumbs --------------------------------------

    public void testMediaThumbLowQuality() {
        assertImage(
                "/wikipedia/it/thumb/0/0d/Venosa-Stemma.png/50px-Venosa-Stemma.png",
                "/wikipedia/it/0/0d/Venosa-Stemma.png", 50);
    }

    public void testMediaThumbHighQuality() {
        assertImage(
                "/wikipedia/commons/thumb/0/01/USS_Texas_BB-35_aircastle.jpg/1024px-USS_Texas_BB-35_aircastle.jpg",
                "/wikipedia/commons/0/01/USS_Texas_BB-35_aircastle.jpg", 1024);
    }

    public void testMediaThumbGif() {
        assertImage(
                "/wikipedia/ar/thumb/c/c1/Logo_of_the_African_Union.png/60px-Logo_of_the_African_Union.png.gif",
                "/wikipedia/ar/c/c1/Logo_of_the_African_Union.png", 60);
    }

    public void testMediaThumbPngJpeg() {
        assertImage(
                "/wikipedia/ru/thumb/2/29/MagicDepartment.png/240px-MagicDepartment.png.jpeg",
                "/wikipedia/ru/2/29/MagicDepartment.png", 240);
    }

    public void testMediaThumbDjvu() {
        assertImage(
                "/wikipedia/commons/thumb/b/b5/foo.djvu/page1-800px-thumbnail.djvu.jpg",
                "/wikipedia/commons/b/b5/foo.djvu", 800);
    }

    public void testMediaThumbSvgPng() {
        assertImage(
                "/wikipedia/commons/thumb/a/ae/Flag_of_the_United_Kingdom.svg/24px-Flag_of_the_United_Kingdom.svg.png",
                "/wikipedia/commons/a/ae/Flag_of_the_United_Kingdom.svg", 24);
    }

    public void testMovieThumbWithoutFormatEnding() {
        assertImage(
                "/wikipedia/commons/thumb/9/9f/Chicago_-_State_St_at_Madison_Ave%2C_1897.ogv/180px-Chicago_-_State_St_at_Madison_Ave%2C_1897.ogv",
                "/wikipedia/commons/9/9f/Chicago_-_State_St_at_Madison_Ave,_1897.ogv",
                180);
    }

    public void testUpperCaseOriginal() {
        assertOriginal("/wikipedia/meta/7/74/Wikibooks-logo_sister_1x.PNG");
    }

    public void testUpperCaseThumbWithoutOwnExtension() {
        assertImage(
                "/wikipedia/commons/thumb/9/93/Rathaus_Wittenberg.JPG/440px-Rathaus_Wittenberg.JPG",
                "/wikipedia/commons/9/93/Rathaus_Wittenberg.JPG",
                440
                );
    }

    // Test uploaded media files; Specialities --------------------------------

    public void testMediaThumbQLow() {
        assertImage(
                "/wikipedia/commons/thumb/8/8c/Google_Mountain_View_campus_garden.jpg/qlow-330px-Google_Mountain_View_campus_garden.jpg",
                "/wikipedia/commons/8/8c/Google_Mountain_View_campus_garden.jpg",
                330);
    }


    public void testMediaThumbMid() {
        assertImage(
                "/wikipedia/commons/thumb/7/7d/Will_Success_Spoil_Rock_Hunter_trailer.ogv/mid-Will_Success_Spoil_Rock_Hunter_trailer.ogv.jpg",
                "/wikipedia/commons/7/7d/Will_Success_Spoil_Rock_Hunter_trailer.ogv",
                null);
    }

    public void testMediaSeek() {
        assertImage(
                "/wikipedia/commons/thumb/3/3d/Suez_nationalization.ogv/seek%3D151-Suez_nationalization.ogv.jpg",
                "/wikipedia/commons/3/3d/Suez_nationalization.ogv",
                null);
    }

    public void testMediaSeekWithResolution() {
        assertImage(
                "/wikipedia/commons/thumb/3/3d/Suez_nationalization.ogv/1000px-seek%3D151-Suez_nationalization.ogv.jpg",
                "/wikipedia/commons/3/3d/Suez_nationalization.ogv", 1000);
    }

    public void testMediaThumbWithoutNameDuplication() {
        assertImage(
                "/wikipedia/commons/thumb/8/8c/Google_Mountain_View_campus_garden.jpg/330px-thumbnail.jpg",
                "/wikipedia/commons/8/8c/Google_Mountain_View_campus_garden.jpg",
                330);
    }

    public void testMediaThumbPdfWithoutNameDuplication() {
        assertImage(
                "/wikipedia/commons/thumb/2/21/Quetzalcatl_-_Divindade_adorada_pelos_Asteca_Tolteca_e_Maias_quem_teria_no_s_originado_os_homens_como_tambm_providenciado_seu_principal_alimento_o_milho.pdf/page1-220px-thumbnail.pdf.jpg",
                "/wikipedia/commons/2/21/Quetzalcatl_-_Divindade_adorada_pelos_Asteca_Tolteca_e_Maias_quem_teria_no_s_originado_os_homens_como_tambm_providenciado_seu_principal_alimento_o_milho.pdf",
                220);
    }

    public void testMediaThumbTifWithoutNameDuplication() {
        assertImage(
                "/wikipedia/commons/thumb/7/72/EXTERIOR_DETAIL_VIEW_OF_THE_UMBRA_FROM_THE_SOUTH_-_Mark_Twain_House_351_Farmington_Avenue_corrected_from_original_address_of_531_Farmington_Avenue_Hartford_Hartford_HABS_CONN-HARF16-30.tif/lossy-page1-120px-thumbnail.tif.jpg",
                "/wikipedia/commons/7/72/EXTERIOR_DETAIL_VIEW_OF_THE_UMBRA_FROM_THE_SOUTH_-_Mark_Twain_House_351_Farmington_Avenue_corrected_from_original_address_of_531_Farmington_Avenue_Hartford_Hartford_HABS_CONN-HARF16-30.tif",
                120);
    }

    public void testMediaThumbSvgWithoutNameDuplication() {
        assertImage(
                "/wikipedia/commons/thumb/6/6e/ABS-6457.0-InternationalTradePriceIndexesAustralia-ExportPriceIndexBySitcIndexNumbersPercentageChanges-IndexNumbers-ManufacturedGoodsClassifiedChieflyByMaterial6-A2295543A.svg/300px-thumbnail.svg.png",
                "/wikipedia/commons/6/6e/ABS-6457.0-InternationalTradePriceIndexesAustralia-ExportPriceIndexBySitcIndexNumbersPercentageChanges-IndexNumbers-ManufacturedGoodsClassifiedChieflyByMaterial6-A2295543A.svg",
                300);
    }

    public void testMediaThumbOgvWithoutNameDuplication() {
        assertImage(
                "/wikipedia/commons/thumb/e/e8/Putin_talk_2011-12-15_00695-00810_....ogv/250px--thumbnail.ogv.jpg",
                "/wikipedia/commons/e/e8/Putin_talk_2011-12-15_00695-00810_....ogv",
                250);
    }

    public void testMediaThumbTiffWithoutNameDuplication() {
        assertImage(
                "/wikipedia/commons/thumb/a/a1/Queens__Vol._2..._NYPL1693954.tiff/lossy-page1-120px-thumbnail.tiff.jpg",
                "/wikipedia/commons/a/a1/Queens__Vol._2..._NYPL1693954.tiff",
                120);
    }

    public void testMediaThumbLangFr() {
        assertImage(
                "/wikipedia/commons/thumb/8/85/Defaut.svg/langfr-250px-Defaut.svg.png",
                "/wikipedia/commons/8/85/Defaut.svg", 250);
    }

    public void testMediaThumbLangFrHighResolution() {
        assertImage(
                "/wikipedia/commons/thumb/8/85/Defaut.svg/langfr-2500px-Defaut.svg.png",
                "/wikipedia/commons/8/85/Defaut.svg", 2500);
    }

    public void testMediaThumbLangPl() {
        assertImage(
                "/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/langpl-400px-First_Ionization_Energy.svg.png",
                "/wikipedia/commons/1/1d/First_Ionization_Energy.svg", 400);
    }

    public void testMediaThumbLangPlHighResolution() {
        assertImage(
                "/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/langpl-4000px-First_Ionization_Energy.svg.png",
                "/wikipedia/commons/1/1d/First_Ionization_Energy.svg", 4000);
    }

    public void testMediaThumbLangZhHans() {
        assertImage(
                "/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/langzh-hans-400px-First_Ionization_Energy.svg.png",
                "/wikipedia/commons/1/1d/First_Ionization_Energy.svg", 400);
    }

    public void testMediaThumbLangZhHansHighResolution() {
        assertImage(
                "/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/langzh-hans-4000px-First_Ionization_Energy.svg.png",
                "/wikipedia/commons/1/1d/First_Ionization_Energy.svg", 4000);
    }

    public void testMediaThumbLangUpperCase() {
        assertUnidentified("/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/langXr-400px-First_Ionization_Energy.svg.png");
    }

    public void testMediaThumbLangNumber() {
        assertUnidentified("/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/lang7-400px-First_Ionization_Energy.svg.png");
    }

    public void testMediaThumbLangNonALpha() {
        assertUnidentified("/wikipedia/commons/thumb/1/1d/First_Ionization_Energy.svg/lang?-400px-First_Ionization_Energy.svg.png");
    }

    public void testMediaThumbPaged() {
        assertImage(
                "/wikipedia/commons/thumb/6/6a/DiagFuncMacroSyst.pdf/page1-450px-DiagFuncMacroSyst.pdf.jpg",
                "/wikipedia/commons/6/6a/DiagFuncMacroSyst.pdf", 450);
    }

    public void testMediaThumbPagedLossy() {
        assertImage(
                "/wikipedia/commons/thumb/0/02/1969_Afghanistan_Sistan_wind_ripples.tiff/lossy-page1-220px-1969_Afghanistan_Sistan_wind_ripples.tiff.jpg",
                "/wikipedia/commons/0/02/1969_Afghanistan_Sistan_wind_ripples.tiff",
                220);
    }

    public void testMediaThumbPagedLossless() {
        assertImage(
                "/wikipedia/commons/thumb/b/b2/KeyCard.tiff/lossless-page1-220px-KeyCard.tiff.png",
                "/wikipedia/commons/b/b2/KeyCard.tiff", 220);
    }

    public void testMediaThumbDoubleDash() {
        assertImage("http://upload.wikimedia.org/wikipedia/commons/thumb/1/1a/Reichelt.ogg/220px--Reichelt.ogg.jpg",
                "/wikipedia/commons/1/1a/Reichelt.ogg", 220);
    }

    public void testMediaThumbSeekInteger() {
        assertImage(
                "/wikipedia/commons/thumb/d/df/Emu_feeding_on_grass.ogg/220px-seek%3D43-Emu_feeding_on_grass.ogg.jpg",
                "/wikipedia/commons/d/df/Emu_feeding_on_grass.ogg", 220);
    }

    public void testMediaThumbSeekFraction() {
        assertImage(
                "/wikipedia/commons/thumb/a/af/Pelecanus_occidentalis_-Jamaica_-fishing-8.ogv/220px-seek%3D2.5-Pelecanus_occidentalis_-Jamaica_-fishing-8.ogv.jpg",
                "/wikipedia/commons/a/af/Pelecanus_occidentalis_-Jamaica_-fishing-8.ogv",
                220);
    }

    public void testMediaThumbTemp() {
        assertImage(
                "/wikipedia/commons/thumb/temp/5/57/20141026110003%21phphHKAQ2.jpg/100px-20141026110003%21phphHKAQ2.jpg",
                "/wikipedia/commons/temp/5/57/20141026110003!phphHKAQ2.jpg",
                100);
    }

    public void testMistranscodedUrl() {
        assertUnidentified("/wikipedia/commons/transcoded/b/bf/foo.ogv/foo.ogv");
    }

    // Test uploaded media files; Archive -------------------------------------

    public void testMediaArchive() {
        assertOriginal(
                "/wikipedia/sr/archive/2/25/20121208204804!ZvezdeGranda.jpg",
                "/wikipedia/sr/archive/2/25/20121208204804!ZvezdeGranda.jpg");
    }

    public void testMediaArchiveEncoded() {
        assertOriginal(
                "/wikipedia/sr/archive/2/25/20121208204804%21ZvezdeGranda.jpg",
                "/wikipedia/sr/archive/2/25/20121208204804!ZvezdeGranda.jpg");
    }

    public void testMediaThumbArchive() {
        assertImage(
                "/wikipedia/commons/thumb/archive/b/bb/20100220202202!Polska_1386_-_1434.png/120px-Polska_1386_-_1434.png",
                "/wikipedia/commons/archive/b/bb/20100220202202!Polska_1386_-_1434.png",
                120);
    }

    public void testMediaThumbArchiveEncoded() {
        assertImage(
                "/wikipedia/commons/thumb/archive/b/bb/20100220202202%21Polska_1386_-_1434.png/120px-Polska_1386_-_1434.png",
                "/wikipedia/commons/archive/b/bb/20100220202202!Polska_1386_-_1434.png",
                120);
    }

    // Test uploaded media files; Transcoded ----------------------------------

    public void testMediaTranscodedWebM() {
        assertMovie(
                "/wikipedia/commons/transcoded/3/31/Lheure_du_foo.ogv/Lheure_du_foo.ogv.360p.webm",
                "/wikipedia/commons/3/31/Lheure_du_foo.ogv", 360);
    }

    public void testMediaTranscodedWebMHighResolution() {
        assertMovie(
                "/wikipedia/commons/transcoded/3/31/Lheure_du_foo.ogv/Lheure_du_foo.ogv.480p.webm",
                "/wikipedia/commons/3/31/Lheure_du_foo.ogv",
                480);
    }

    public void testMediaTranscodedOgv() {
        assertMovie(
                "/wikipedia/commons/transcoded/d/d3/Yvonne_Strahovski_about_her_acting_career.webm/Yvonne_Strahovski_about_her_acting_career.webm.360p.ogv",
                "/wikipedia/commons/d/d3/Yvonne_Strahovski_about_her_acting_career.webm",
                360);
    }

    public void testMediaTranscodedOgvHighResolution() {
        assertMovie(
                "/wikipedia/commons/transcoded/d/d3/Yvonne_Strahovski_about_her_acting_career.webm/Yvonne_Strahovski_about_her_acting_career.webm.480p.ogv",
                "/wikipedia/commons/d/d3/Yvonne_Strahovski_about_her_acting_career.webm",
                480);
    }

    public void testMediaTranscodedOgg() {
        assertAudio(
                "/wikipedia/commons/transcoded/b/bd/Xylophone_jingle.wav/Xylophone_jingle.wav.ogg",
                "/wikipedia/commons/b/bd/Xylophone_jingle.wav");
    }
}
