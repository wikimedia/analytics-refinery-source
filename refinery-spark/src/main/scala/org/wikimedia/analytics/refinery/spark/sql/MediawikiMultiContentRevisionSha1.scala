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
     * Spark Rows implementation of MediaWiki's RevisionSlots::computeSha1().
     * It computes a re-hashed sha1 in base 36 of multi-content revisions sha1.
     * Original Mediawiki code:
     * https://github.com/wikimedia/mediawiki/blob/14cb7eed0fcfd3f6d6a4ced7145202b6c7c32c68/includes/Revision/RevisionSlots.php#L195-L208
     *
     * @param contentRolesAndSha1s A list of structured pairs of the form {slot_role_name, sha1}
     * @return <li>null if the content list is null or empty
     *         <li>the single content sha1 value if the content list contains one element
     *         <li>the recomputed sha1 if the content list contains more than one element
     */
    def computeForRows(contentRolesAndSha1s: Seq[Row]): String = {
      if (contentRolesAndSha1s == null || contentRolesAndSha1s.isEmpty) {
        null
      } else {
        computeForTuples(contentRolesAndSha1s.map(row => (row.getString(0), row.getString(1))))
      }
    }

    /**
     * Scala implementation of MediaWiki's RevisionSlots::computeSha1().
     * It computes a re-hashed sha1 in base 36 of multi-content revisions sha1.
     * Original Mediawiki code:
     * https://github.com/wikimedia/mediawiki/blob/14cb7eed0fcfd3f6d6a4ced7145202b6c7c32c68/includes/Revision/RevisionSlots.php#L195-L208
     *
     * @param contentRolesAndSha1s A list of structured pairs of the form {slot_role_name, sha1}
     * @return <li>null if the content list is null or empty
     *         <li>the single content sha1 value if the content list contains one element
     *         <li>the recomputed sha1 if the content list contains more than one element
     */
    def computeForTuples(contentRolesAndSha1s: Seq[(String, String)]): String = {
        if (contentRolesAndSha1s == null || contentRolesAndSha1s.isEmpty) {
            null
        } else if (contentRolesAndSha1s.length == 1) {
            contentRolesAndSha1s.head._2
        } else {
            contentRolesAndSha1s
                .sortBy(_._1)
                .foldLeft("")((acc: String, contentRoleAndSha1: (String, String)) =>{
                    if (acc.isEmpty) {
                      contentRoleAndSha1._2
                    } else {
                        // Concatenate the accumulated sha1 and the new sha1, recompute a sha1 of
                        // this string, parse it in base 16 and export it in base 36.
                        // We need to pad the result with 0s, as mediawiki computes a 31 chars value
                        String.format("%31s", math.BigInt(sha1(acc.concat(contentRoleAndSha1._2)), 16).toString(36)).replace(' ', '0')
                    }
                })
        }
    }
}
