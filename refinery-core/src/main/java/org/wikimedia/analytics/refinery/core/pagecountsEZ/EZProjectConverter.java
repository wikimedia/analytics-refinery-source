package org.wikimedia.analytics.refinery.core.pagecountsEZ;

public class EZProjectConverter {
    public static String ezProjectToStandard (String ezProject) {
        String lowerCasedName = ezProject.toLowerCase();
        String language;
        String family;
        String standardFamily;
        String[] parts = lowerCasedName.split("\\.");
        if (parts.length == 2) {
            language = parts[0];
            family = parts[1];
            if (family.equals("mw")) return null; // mw represents the sum of mobile views for a language and we don't count it.
            standardFamily = lettersToWikiFamily(family);
            if (language.equals("www")) return standardFamily;
            if (standardFamily.equals("wikidata")) return "wikidata"; // old wikidata counts might contain a language subdomain
        } else if (parts.length == 1) {
            language = lowerCasedName;
            standardFamily = "wikipedia";
        } else {
            throw new IllegalArgumentException("EZ-formatted project string must either contain a period (.) or be a single word");
        }
        return language + '.' + standardFamily;
    }

    private static String lettersToWikiFamily(String letters) {
        String code = null;
        switch (letters) {
            case "z":
            case "y":
                code = "wikipedia"; break;
            case "d":
                code = "wiktionary"; break;
            case "n":
                code = "wikinews"; break;
            case "m":
                code = "wikimedia"; break;
            case "b":
                code = "wikibooks"; break;
            case "s":
                code = "wikisource"; break;
            case "w":
                code = "mediawiki"; break;
            case "v":
                code = "wikiversity"; break;
            case "q":
                code = "wikiquote"; break;
            case "voy":
                code = "wikivoyage"; break;
            case "f":
                code = "wikimediafoundation"; break;
            case "wd":
                code = "wikidata"; break;
        }
        return code;
    }
}
