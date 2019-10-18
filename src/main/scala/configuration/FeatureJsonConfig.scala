package configuration

/**
  * Created by 58 on 2018/3/7.
  */
object FeatureJsonConfig {

  val RAW_SAMPLE_INPATH = "inPath"
  val SAMPLE_OUTPATH = "outPath"
  val BOUNDS_INPATH = "boundsInPath"
  val TAGVECTOR_PATH = "tagCateVectors_path"
  val TAGSIMILIRITY_PATH = "tagCateSimilarities_path"
  val SAMPLE_TYPE = "sampleType"
  val LABEL_TYPE = "labelType"
  val HAS_HEADER = "hasHeader"
  val SLOT = "slot"
  val DATE = "date"
  val OUT_FORMAT = "outFormat"


  // 关联user和buser特征时会数据倾斜
  val DATA_SKEW = "dataSkew"
  val USER_EXTNUM_DATA_SKEW = "userExtNum"
  val BUSER_EXTNUM_DATA_SKEW = "buserExtNum"

  // feature type
  val FEATURE = "feature"
  val FEATURE_CONTEXT = "context"
  val FEATURE_INFO_BASIC = "info"
  val FEATURE_INFO_STAT = "infostat"
  val FEATURE_USER_BASIC = "user"
  val FEATURE_USER_STAT = "userstat"
  val FEATURE_USER_PREFER = "userprefer"
  val FEATURE_ENTERPRISE_BASIC = "enterprise"
  val FEATURE_ENTERPRISE_STAT = "enterprisestat"
  val FEATURE_CONTEXT_FEATURE= "contextfeature"

  val FEATURE_CROSS = "cross"
  val FEATURE_CROSS_STAT = "crossstat"
  val FEATURE_CROSS_RDD = "crossrdd" // 数据量太大的交叉统计特征需要单独拎出来,使用RDD join API做关联
  val FEATURE_XGBOOST = "xgboost"

  val FUNCTION_TAGSIMI = "tagSimi"

  val FUNCTION_CONTAIN = "contain"
  // feature param type
  val PARAM_SETID = "setid"
  val PARAM_FEATURE_NAME = "fn"
  val PARAM_FEATURE_NAME_COMBINE = "fnComb"
  // 指定函数变换的名称
  val PARAM_FUNCTION_NAME = "funName"
  // 指定函数变换的参数
  val PARAM_FUNCTION_PARAMS = "funParams"
  val PARAM_STAT_TYPE = "st"
  val PARAM_DISC_TYPE = "dt"
  val PARAM_BOUND_NUM = "bn"
  val PARAM_BOUNDS = "bounds"
  val PARAM_DEFAULT = "df" // 默认值
  val PARAM_IS_USE = "isUse"

  // user prefer type  偏好处理方式
  val PARAM_PREFER_COMPARE_FEATURE = "cmp"
  val PARAM_PREFER_COMPARE_TYPE = "cmpType"
  val PARAM_PREFER_FIXED = "fixed" // 固定值
  val PARAM_PREFER_COMPARE_ORG = "org" //原始值输出

  // discretization type
  val DISC_EQUAL_FIELD = "equalField"
  val DISC_SAME_NUM = "sameNum"
  val DISC_SIMI_CTR = "simiCtr"
  val DISC_INFO_GAIN = "infoGain"
}
