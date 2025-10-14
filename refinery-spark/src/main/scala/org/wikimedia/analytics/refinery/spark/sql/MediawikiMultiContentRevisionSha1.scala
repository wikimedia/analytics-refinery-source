package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql.Row

import java.security.MessageDigest

object MediawikiMultiContentRevisionSha1 {

    private def sha1(input: String): String = {
        val md = MessageDigest.getInstance("SHA-1")
        val digest = md.digest(input.getBytes("UTF-8"))
        digest.map("%02x".format(_)).mkString
    }

    /**
     * Function computing the re-hashed sha1 in base 36 of multi-content revisions sha1.
     * @param contentRolesAndSha1 A list of structured pairs of the form {slot_role_name, sha1}
     * @return <li>null if the content list is null or empty
     *         <li>the single content sha1 value if the content list contains one element
     *         <li>the recomputed sha1 if the content list contains more than one element
     */
    def compute(contentRolesAndSha1: Seq[Row]): String = {
        if (contentRolesAndSha1 == null || contentRolesAndSha1.isEmpty) {
            null
        } else if (contentRolesAndSha1.length == 1) {
            contentRolesAndSha1.head.getAs[String](1)
        } else {
            contentRolesAndSha1
                .sortBy((r: Row) => r.getString(0))
                .foldLeft("")((acc: String, row: Row) =>{
                    if (acc.isEmpty) {
                        row.getString(1)
                    } else {
                        // Concatenate the accumulated sha1 and the new sha1, recompute a sha1 of
                        // this string, parse it in base 16 and export it in base 36.
                        // We need to pad the result with 0s, as mediawiki computes a 31 chars value
                        String.format("%31s", math.BigInt(sha1(acc.concat(row.getString(1))), 16).toString(36)).replace(' ', '0')
                    }
                })
        }
    }
}
