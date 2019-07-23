package org.wikimedia.analytics.refinery.core.media;

/**
 * A POJO to store the
 * two basic attributes of a Media File:
 * fileExtension and parentType
 */
public class MediaType {

    private String fileExtension;

    private  String parentType  = null;

    public MediaType(String fileExtension){

        for (MediaTypeClassification mt: MediaTypeClassification.values() ){
            if (mt.name().equalsIgnoreCase(fileExtension.trim())) {
                this.setParentType( mt.getParentType());
                this.setFileExtension(mt.getCanonicalExtension());
                break;
            }
        }

        if (this.parentType == null){
            this.setParentType(MediaTypeClassification.OTHER.name().toLowerCase());
            this.setFileExtension(fileExtension.trim().toLowerCase());
        }

    }

    public String getParentType(){
        return parentType;
    }

    public void setParentType(String parentType){
        this.parentType = parentType;
    }

    public String getFileExtension(){
        return fileExtension;
    }

    public void setFileExtension(String fileExtension){
        this.fileExtension = fileExtension;
    }

}
