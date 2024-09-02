package cn.dev33.satoken.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.walltech.cfs.act.dao.overseas.warehouse.*;
import com.walltech.cfs.act.dict.*;
import com.walltech.cfs.act.pojo.dto.ActHandleReturnDetailDTO;
import com.walltech.cfs.act.pojo.dto.ActLastMileRateCalculateDTO;
import com.walltech.cfs.act.pojo.dto.ActRateAndItemDTO;
import com.walltech.cfs.act.pojo.dto.overseas_warehouse.*;
import com.walltech.cfs.act.pojo.entity.SysChinaBankExchangeRate;
import com.walltech.cfs.act.pojo.entity.overseas_warehouse.*;
import com.walltech.cfs.act.pojo.request.overseas_warehouse.ActLastMileTaxRequest;
import com.walltech.cfs.act.pojo.request.overseas_warehouse.create.ActOverseasWarehouseCostCreate;
import com.walltech.cfs.act.pojo.vo.ActRateDetailVo;
import com.walltech.cfs.auth.util.WalltechSignUtil;
import com.walltech.cfs.base.dao.*;
import com.walltech.cfs.base.dict.BusinessNoType;
import com.walltech.cfs.base.dict.ChannelType;
import com.walltech.cfs.base.idworker.BusinessNo;
import com.walltech.cfs.base.pojo.dto.*;
import com.walltech.cfs.base.pojo.entity.*;
import com.walltech.cfs.base.util.AWSFileUtils;
import com.walltech.cfs.base.util.AddressUtils;
import com.walltech.cfs.lastmile.pojo.vo.OrderCloseRequestVO;
import com.walltech.cfs.order.dao.*;
import com.walltech.cfs.order.dict.*;
import com.walltech.cfs.order.pojo.dto.*;
import com.walltech.cfs.order.pojo.dto.wms.WmsStorageAgingDTO;
import com.walltech.cfs.order.pojo.entity.*;
import com.walltech.cfs.order.pojo.request.*;
import com.walltech.cfs.order.pojo.request.wms.WmsInventoryAgingQueryRequest;
import com.walltech.cfs.order.pojo.vo.*;
import com.walltech.cfs.order.pojo.vo.operationlog.AttachmentForLogVO;
import com.walltech.cfs.order.result.code.*;
import com.walltech.cfs.order.util.BigDecimalUtils;
import com.walltech.cfs.order.util.UnitUtil;
import com.walltech.cfs.sys.aop.log.Annotation.OperationLog;
import com.walltech.cfs.sys.feign.ActLastMileRateHelper;
import com.walltech.cfs.sys.feign.wms.WmsOutboundOrderServiceFeign;
import com.walltech.cfs.sys.kafka.consumer.bean.B2CFulfillmentOrderCallBackBean;
import com.walltech.cfs.sys.pojo.request.B2cOrderDispatchRequest;
import com.walltech.cfs.sys.pojo.request.B2cOrderSplitRequest;
import com.walltech.cfs.sys.pojo.vo.B2cOrderSplitVO;
import com.walltech.cfs.sys.security.properties.B2cOrderProperties;
import com.walltech.cfs.sys.service.WmsHandleStrategyService;
import com.walltech.cfs.sys.service.act.ActLastMileCustomSurchargeService;
import com.walltech.cfs.sys.service.act.SysChinaBankExchangeRateService;
import com.walltech.cfs.sys.service.act.overseas_warehouse.ActLastMileCalculateFeeService;
import com.walltech.cfs.sys.service.act.overseas_warehouse.ActOverseasWarehouseAccountService;
import com.walltech.cfs.sys.service.act.overseas_warehouse.ActOverseasWarehouseBillService;
import com.walltech.cfs.sys.service.act.overseas_warehouse.ActOverseasWarehouseCalculateFeeService;
import com.walltech.cfs.sys.service.act.utils.ActRateHelper;
import com.walltech.cfs.sys.service.base.*;
import com.walltech.cfs.sys.service.base.impl.NumberGenerateService;
import com.walltech.cfs.sys.service.lastmile.ETowerOrderService;
import com.walltech.cfs.sys.service.order.*;
import com.walltech.cfs.sys.service.redis.RedisLock;
import com.walltech.cfs.sys.service.third.party.b2c.B2cShipperService;
import com.walltech.cfs.sys.service.third.party.fedex.FedexService;
import com.walltech.cfs.sys.utils.RateCalculateUtils;
import com.walltech.cfs.sys.utils.RateCheckUtils;
import com.walltech.cfs.sys.utils.ZoneCheckUtils;
import com.walltech.cfs.third.party.dict.FedexValidationAddressClassification;
import com.walltech.cfs.third.party.dict.ThirdApiBusinessType;
import com.walltech.cfs.third.party.pojo.dto.WmsCommonRespDTO;
import com.walltech.cfs.third.party.pojo.dto.b2c.createOrder.B2cCreateOrderResponseDTO;
import com.walltech.cfs.third.party.pojo.dto.b2c.rateInquiry.B2cRateInquiryForSpecialSurchargeDTO;
import com.walltech.cfs.third.party.pojo.dto.b2c.rateInquiry.B2cRateInquiryPiecesDTO;
import com.walltech.cfs.third.party.pojo.dto.b2c.rateInquiry.B2cRateInquiryRequestDTO;
import com.walltech.cfs.third.party.pojo.dto.b2c.rateInquiry.B2cRateInquiryResponseDTO;
import com.walltech.cfs.third.party.pojo.dto.common.order.WmsCreateUpdateOrderDTO;
import com.walltech.cfs.third.party.pojo.dto.common.order.WmsCreateUpdateOrderItemDTO;
import com.walltech.cfs.third.party.pojo.dto.fedex.validationAddress.FedexValidationAddressResponse;
import com.walltech.cfs.third.party.pojo.dto.wms.cancelOrder.WmsCancelOrderResultDTO;
import com.walltech.cfs.third.party.pojo.dto.wms.createOrder.*;
import com.walltech.cfs.third.party.pojo.dto.wms.uploadFile.WmsUploadFileRequestDTO;
import com.walltech.cfs.third.party.pojo.dto.wms.uploadFile.WmsUploadFileResponseDTO;
import com.walltech.cfs.track.dict.WareHouseOrderEventCode;
import com.walltech.cfs.track.pojo.dto.TrkSourceOrderEventDTO;
import com.walltech.common.aop.annotation.processor.FieldTrimProcessor;
import com.walltech.common.context.SessionContext;
import com.walltech.common.dict.StatusType;
import com.walltech.common.exception.ExUtil;
import com.walltech.common.exception.SubError;
import com.walltech.common.export.TemplatePdfExporter;
import com.walltech.common.model.annotation.CurrencyCheck;
import com.walltech.common.model.annotation.ValidateBigDecimal;
import com.walltech.common.mybatis.interceptor.CountBaseEntity;
import com.walltech.common.mybatis.interceptor.CountHelper;
import com.walltech.common.result.code.SystemResultCode;
import com.walltech.common.service.LogService;
import com.walltech.common.service.RedissonService;
import com.walltech.common.utils.*;
import com.walltech.common.web.Result;
import com.walltech.mq.producer.kafka.WallTechKafkaProducer;
import com.xxl.job.core.log.XxlJobLogger;
import lombok.extern.slf4j.Slf4j;
import net.sf.jasperreports.engine.data.JRBeanCollectionDataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.hibernate.validator.constraints.Length;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import tk.mybatis.mapper.entity.Example;
import tk.mybatis.mapper.util.Sqls;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class PclOrderB2cServiceImpl implements PclOrderB2cService, LogService<PclOrderB2cForLogVO,Long> {

    @Resource
    private PclOrderB2cDao pclOrderB2cDao;

    @Resource
    private SysProductDao sysProductDao;

    @Resource
    private SysOverseasWarehouseDao sysOverseasWarehouseDao;

    @Resource
    private SysWarehouseDao sysWarehouseDao;

    @Resource
    private PclInventoryDao pclInventoryDao;

    @Resource
    private SysAddressDao sysAddressDao;

    @Resource
    private PclProductB2cDao pclProductB2cDao;

    @Resource
    private PclBatchDao pclBatchDao;

    @Resource
    private ActStorageFeeRateDao actStorageFeeRateDao;

    @Resource
    private SysOverseasWarehouseChannelDao sysOverseasWarehouseChannelDao;

    @Resource
    private SysOverseasWarehouseShipperDao sysOverseasWarehouseShipperDao;

    @Resource
    private SysPartyDao sysPartyDao;

    @Resource
    private PclInventoryAgingDao pclInventoryAgingDao;


    @Resource
    private B2cOrderProperties b2cOrderProperties;

    @Resource
    private PclInventoryLogDao pclInventoryLogDao;

    @Resource
    private SysPartyService sysPartyService;

    @Resource
    private ActOverseasWarehouseRateConfigDao actOverseasWarehouseRateConfigDao;

    @Resource
    private ActStorageFeeRateItemDao actStorageFeeRateItemDao;

    @Resource
    private ActHandleReturnDao actHandleReturnDao;

    @Resource
    private ActOverseasWarehouseBillService actOverseasWarehouseBillService;

    @Resource
    private SysChinaBankExchangeRateService sysChinaBankExchangeRateService;

    @Resource
    private SysAddressService addressService;

    @Resource
    private Set<WmsHandleStrategyService> wmsHandleServiceSet;

    Map<Integer, WmsHandleStrategyService> wmsHandleServiceMap = new HashMap<>();

    @Resource
    private ActLastMileCalculateFeeService lastMileCalculateFeeService;

    @Resource
    private ActLastMileRateDao actLastMileRateDao;

    @Resource
    private PclOrderReturnDao pclOrderReturnDao;

    @Resource
    private ActOverseasWarehouseAccountService actOverseasWarehouseAccountService;

    @Resource
    private SysAddressRuleConfigService sysAddressRuleConfigService;

    @Resource
    private ActOverseasWarehouseCalculateFeeService warehouseCalculateFeeService;

    @Resource
    private SysOperationLogService sysOperationLogService;

    @Resource
    private SysOverseasWarehouseShipperService sysOverseasWarehouseShipperService;

    @Resource
    private PclOrderB2cService pclOrderB2cService;

    @Resource
    private SysAddressRuleConfigDao sysAddressRuleConfigDao;

    @Resource
    private OrderEventService orderEventService;

    @Autowired
    private PclOrderService pclOrderService;

    @Resource
    private B2cShipperService b2cShipperService;

    @Resource
    private PclOrderB2cLabelDao pclOrderB2cLabelDao;

    @Resource
    private ETowerOrderService eTowerOrderService;

    @Resource
    private SysAddressValidationTypeShipperDao sysAddressValidationTypeShipperDao;

    @Resource
    private FedexService fedexService;

    @Resource
    private SysOperationLogDao sysOperationLogDao;

    @Resource
    private ActRateHelper actRateHelper;

    @Resource
    private PclOrderB2cAttachmentDao pclOrderB2cAttachmentDao;

    @Resource
    private ActLastMileRateHelper actLastMileRateHelper;

    @Resource
    private SysAddressService sysAddressService;

    @Resource
    private ActLastMileSurchargeConfigDao lastMileSurchargeConfigDao;

    @Resource
    private ActLastMileSurchargeConfigItemDao lastMileSurchargeConfigItemDao;

    @Resource
    private ActLastMileSurchargeDao actLastMileSurchargeDao;

    @Resource
    private ActLastMileSurchargeFuelItemDao lastMileSurchargeFuelItemDao;

    @Resource
    private SysZoneItemDetailsDao sysZoneItemDetailsDao;

    @Resource
    private ActLastMileTaxDao actLastMileTaxDao;

    @Resource
    private PclInsuranceConfigService pclInsuranceConfigService;

    @Resource
    private PclOrderPoRelationService orderPoRelationService;

    @Resource
    private ActLastMileCustomSurchargeService actLastMileCustomSurchargeService;

    @Resource
    private PclOrderPoRelationService pclOrderPoRelationService;

    @Resource
    private RedisLock redisLock;


    @Resource
    private RedissonService redissonService;

    @Autowired
    private TemplatePdfExporter templatePdfExporter;

    @Resource
    private SysEdiLogService sysEdiLogService;

    @Resource
    private WmsOutboundOrderServiceFeign outboundOrderServiceFeign;

    @Resource
    private WallTechKafkaProducer wallTechKafkaProducer;

    @Value("${etower-order-callback-topic}")
    private String etowerOrderCallbackTopic;

    @PostConstruct
    public void init() {
        for (WmsHandleStrategyService handleService : wmsHandleServiceSet) {
            wmsHandleServiceMap.put(handleService.overseasWarehousePlatformChannel(), handleService);
        }
    }

    private final RestTemplate restTemplate;

    @Autowired
    public PclOrderB2cServiceImpl(RestTemplateBuilder restTemplateBuilder) {
        this.restTemplate = restTemplateBuilder.build();
    }

    @Override
    public PageInfo<PclOrderB2cVO> query(PclOrderB2cRequest request) {
        log.info("处理字符串字段前后空格");
        FieldTrimProcessor.trimObject(request);
        List<Integer> sellers = pclOrderService.getSellers(request.getItemType());
        if(sellers!=null && sellers.size()==0){
            return new PageInfo<>(new ArrayList<>());
        }
        request.setShipperIds(sellers);
        PageHelper.startPage(request.initPage());
        List<Short> status = request.getStatus();
        List<String> referenceNos = request.getReferenceNos();
        List<String> batchNos = request.getBatchNos();
        List<String> orderNos = request.getOrderNos();
        List<String> trackingNos = request.getTrackingNos();
        List<String> platformRefNos = request.getPlatformRefNos();
        if (!Detect.notEmpty(referenceNos)) {
            request.setReferenceNos(null);
        }
        if (!Detect.notEmpty(batchNos)) {
            request.setBatchNos(null);
        }
        if (!Detect.notEmpty(orderNos)) {
            request.setOrderNos(null);
        }
        if (!Detect.notEmpty(trackingNos)) {
            request.setTrackingNos(null);
        }
        if (Detect.notEmpty(status)) {
            request.setIsDelete(status.contains(B2cOrderStatus.ABANDONED.getType()));
        } else {
            request.setIsDelete(false);
        }
        if (!Detect.notEmpty(platformRefNos)) {
            request.setPlatformRefNos(null);
        }
        if(PartyUtil.isShipper()){
            request.setIsShipper(true);
        }else{
            request.setIsShipper(false);
        }
        List<PclOrderB2c> pclOrderB2cList = pclOrderB2cDao.selectByRequest(request);
        PageInfo<PclOrderB2c> pclOrderB2cDTOPageInfo = new PageInfo<>(pclOrderB2cList);
        Map<Integer,PartyVo> partyVoMap = sysPartyService.findShipperMapByAggregator(SessionContext.getContext().getAggregator());
        List<String> orderNoList = pclOrderB2cList.stream().map(p -> p.getOrderNo()).collect(Collectors.toList());
        //查询订单生成的费用
        Map<String, String> orderFeeMap = queryOrderFeeInfoMap(orderNoList);
        //所属入库单和柜号
        Map<String, PclOrderPoRelationDTO> poRelationMap = pclOrderPoRelationService.getPoRelationMap(orderNoList);
        List<PclOrderB2cVO> pclOrderB2cVOList = pclOrderB2cList.stream().map(pclOrderB2c -> {
            PclOrderB2cVO pclOrderB2cVO = BeanUtils.transform(pclOrderB2c, PclOrderB2cVO.class);
            if (pclOrderB2cVO.getStatus() != null) {
                B2cOrderStatus b2cOrderStatus = B2cOrderStatus.get(pclOrderB2cVO.getStatus());
                if (b2cOrderStatus != null) {
                    pclOrderB2cVO.setStatusMsg(b2cOrderStatus.getMessage());
                }
            } else {
                pclOrderB2cVO.setStatusMsg(B2cOrderStatus.UNCONFIRMED.getMessage());
            }
            if(pclOrderB2cVO.getSource()!=null&&OrderSource.get(pclOrderB2cVO.getSource())!=null){
                pclOrderB2cVO.setSourceMsg(OrderSource.get(pclOrderB2cVO.getSource()).getMessage());
            }
            if(pclOrderB2cVO.getPickingType()!=null && OrderPickingType.get(pclOrderB2cVO.getPickingType())!=null){
                pclOrderB2cVO.setPickingTypeMsg(OrderPickingType.get(pclOrderB2cVO.getPickingType()).getMessage());
            }
            PartyVo partyVo = partyVoMap.get(pclOrderB2c.getShipperId());
            if(Objects.nonNull(partyVo)){
                pclOrderB2cVO.setShipperName(partyVo.getNameLocal());
                pclOrderB2cVO.setShipperNameEn(partyVo.getNameEn());
            }
            YesOrNoType yesOrNoType = YesOrNoType.get(pclOrderB2c.getServiceChanged());
            pclOrderB2cVO.setServiceChangedMsg(yesOrNoType.getMessage());
            //是否一票多件
            String trackingNo = pclOrderB2c.getTrackingNo();
            if (Detect.notEmpty(trackingNo)) {
                if (trackingNo.contains(Constants.SplitConstant.SEMICOLON)) {
                    pclOrderB2cVO.setIsMultiPackage(true);
                } else {
                    pclOrderB2cVO.setIsMultiPackage(false);
                }
            }
            //账单费用
            String fee = orderFeeMap.get(pclOrderB2c.getOrderNo());
            pclOrderB2cVO.setFee(fee);
            //所属入库单信息
            PclOrderPoRelationDTO pclOrderPoRelationDTO = poRelationMap.get(pclOrderB2c.getOrderNo());
            if (pclOrderPoRelationDTO != null) {
                pclOrderB2cVO.setFromPoOrderNos(pclOrderPoRelationDTO.getPoOrderNo());
                pclOrderB2cVO.setContainerNos(pclOrderPoRelationDTO.getContainerNo());
            }
            return pclOrderB2cVO;
        }).collect(Collectors.toList());
        PageInfo<PclOrderB2cVO> pclOrderB2cVOPageInfo = new PageInfo<>(pclOrderB2cVOList);
        BeanUtils.transform(pclOrderB2cDTOPageInfo, pclOrderB2cVOPageInfo);
        pclOrderB2cVOPageInfo.setList(pclOrderB2cVOList);
        return pclOrderB2cVOPageInfo;
    }


    /**
     * 根据订单号 查询订单关联的费用
     * @param orderNos
     * @return
     */
    private Map<String, String> queryOrderFeeInfoMap(List<String> orderNos) {
        if (Detect.empty(orderNos)) {
            return Maps.newHashMap();
        }
        Map<String, String> feeMap = Maps.newHashMap();
        List<ActOverseasWarehouseBill> warehouseBillList = actOverseasWarehouseBillService.getByOrderNos(orderNos);
        warehouseBillList.stream().collect(Collectors.groupingBy(p -> p.getOrderNo()))
                .forEach((orderNo, warehouseBills) ->{
                    List<String> feeList = Lists.newArrayList();
                    AtomicReference<Boolean> hasNullFlag = new AtomicReference<>(false);
                    warehouseBills.forEach(e -> {
                        if(Objects.isNull(e.getConvertCurrency())){
                            hasNullFlag.getAndSet(true);
                        }
                    });
                    if(hasNullFlag.get()){
                        return;
                    }
                    warehouseBills.stream().collect(Collectors.groupingBy(p -> p.getConvertCurrency()))
                            .forEach((currency, oneCurrencyWarehouseBills) ->{
                                BigDecimal oneCurrencyAmount = oneCurrencyWarehouseBills.stream().map(p -> BigDecimalUtils.covertNullToZero(p.getConvertAmount())).reduce(BigDecimal.ZERO, BigDecimal::add);
                                feeList.add(oneCurrencyAmount + currency);
                            });
                    feeMap.put(orderNo, feeList.stream().collect(Collectors.joining(";")));
        });
        return feeMap;
    }

    @Override
    public PclOrderB2c get(Long id) {
        return pclOrderB2cDao.selectByPrimaryKey(id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public List<SubError> uploadB2cOrder(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, PclOrderB2cRequest pclOrderB2cRequest) {
        List<SubError> errors = new ArrayList<>();
        Integer shipperId = pclOrderB2cRequest.getShipperId();
        PartyVo shipper = sysPartyService.findByPartyId(shipperId);
        //效验必填值
        checkNotNullFields(pclOrderB2cUploadDTOList, errors);
        if (Detect.notEmpty(errors)) {
            return errors;
        }
        validateUploadB2cOrderParam(pclOrderB2cUploadDTOList, errors);
        if(Detect.notEmpty(errors)){
            return errors;
        }
        pclOrderB2cUploadDTOList.forEach(pclOrderB2cUploadDTO -> pclOrderB2cUploadDTO.setShipperId(shipperId));
        //效验订单号相同情况下。其余信息不同的的情况
        checkSameReferenceNoOrder(pclOrderB2cUploadDTOList,shipperId, errors);
        if (Detect.notEmpty(errors)) {
            return errors;
        }
        //转化导入的信息
        List<PclOrderB2cThirdDTO> pclOrderB2cDTOList = covertToPclOrderB2cDTO(pclOrderB2cUploadDTOList, shipper, errors);

        //判断发货人+ 发货仓库 对应的币种账户是否已经冻结
        checkShipperWarehouseFrozen(pclOrderB2cDTOList, errors);
        if (!Detect.notEmpty(pclOrderB2cDTOList) || Detect.notEmpty(errors)) {
            return errors;
        }
        //调用b2c创建订单接口
        createB2cOrder(pclOrderB2cDTOList, shipper);
        return errors;
    }

    private void validateUploadB2cOrderParam(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, List<SubError> errors){
        for (int i = 0; i < pclOrderB2cUploadDTOList.size(); i++) {
            String rowNum = String.valueOf(i + 1);
            PclOrderB2cUploadDTO uploadDTO = pclOrderB2cUploadDTOList.get(i);
            Field[] fields = uploadDTO.getClass().getDeclaredFields();
            for (Field field : fields) {
                String fieldName = field.getName();
                field.setAccessible(true);
                Object fieldValue = null;
                try {
                    fieldValue = field.get(uploadDTO);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                //数字验证
                ValidateBigDecimal validateBigDecimalAnn = field.getAnnotation(ValidateBigDecimal.class);
                if (validateBigDecimalAnn != null) {
                    ValidationUtils.validateBigDecimal(errors, fieldValue, validateBigDecimalAnn.rex(), CommonUploadResultCode.COMMON_UPLOAD_FORMAT_ERROR, rowNum, I18nMessageHelper.getMessage(fieldName));
                }
                //长度验证
                Length length = field.getAnnotation(Length.class);
                if (length != null) {
                    ValidationUtils.validateLength(errors, fieldValue, length.min(), length.max(), CommonUploadResultCode.COMMON_UPLOAD_LENGTH_EXCEED_ERROR,
                            rowNum, I18nMessageHelper.getMessage(fieldName), String.valueOf(length.min()), String.valueOf(length.max()));
                }

                CurrencyCheck currencyCheckAnn = field.getAnnotation(CurrencyCheck.class);
                if (currencyCheckAnn != null) {
                    ValidationUtils.validateCurrency(errors, fieldValue, CommonUploadResultCode.COMMON_UPLOAD_VALUE_DOES_NOT_EXIST_ERROR, rowNum, I18nMessageHelper.getMessage(fieldName));
                }
            }
        }
    }

    /**
     * 校验上传订单数据
     * @param pclOrderB2cUploadDTOList
     * @param errors
     */
    @Override
    public HashMap<String, List<SysAddressRuleConfig>> checkUploadRecipientAddress(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, Short channelType, List<SubError> errors) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        //查询收货人地址代码的地址信息
        Set<String> recipientWarehouseCodes = pclOrderB2cUploadDTOList.stream().map(p -> p.getRecipientWarehouseCode()).collect(Collectors.toSet());
        Map<String, SysWarehouse> warehouseCodeMap = Maps.newHashMap();
        if(Detect.notEmpty(recipientWarehouseCodes)){
            List<SysWarehouse> sysWarehouseList = sysWarehouseDao.findByCodes(new ArrayList<>(recipientWarehouseCodes), aggregator);
            warehouseCodeMap= sysWarehouseList.stream().collect(Collectors.groupingBy(p -> p.getCode(),
                    Collectors.collectingAndThen(Collectors.toList(), value -> value.get(0))));
        }
        //根据虚拟服务名称 查询 虚拟服务代码 放入缓存map
        List<SubError> errorList = Lists.newArrayList();
        HashMap<String, String> cacheMap = Maps.newHashMap();
        for (PclOrderB2cUploadDTO uploadDTO : pclOrderB2cUploadDTOList){
            String virtualChannelName = uploadDTO.getVirtualChannelName();
            String shipperWarehouseCode = uploadDTO.getShipperWarehouseCode();
            String key = virtualChannelName + " || " + shipperWarehouseCode;
            String value = cacheMap.get(key);
            if("".equals(value)){
                //缓存中value为空串表示服务在海外仓中不存在
                errorList.add(SubError.build(PclOrderB2cResultCode.CHANNEL_NOT_EXIST_WAREHOUSE, uploadDTO.getRowNum()));
            }else if(value == null){
                //第一次查询
                SysOverseasWarehouseChannelDTO sysOverseasWarehouseChannelDTO = sysOverseasWarehouseChannelDao.selectByWarehouseAndChannelName(shipperWarehouseCode, virtualChannelName, aggregator, channelType);
                if(sysOverseasWarehouseChannelDTO == null){
                    errorList.add(SubError.build(PclOrderB2cResultCode.CHANNEL_NOT_EXIST_WAREHOUSE, uploadDTO.getRowNum()));
                    cacheMap.put(key, "");
                }else{
                    String virtualChannelCode = sysOverseasWarehouseChannelDTO.getVirtualChannelCode();
                    uploadDTO.setVirtualChannelCode(virtualChannelCode);
                    cacheMap.put(key, virtualChannelCode);
                }
            }else {
                //从缓存中取
                uploadDTO.setVirtualChannelCode(value);
            }
            //处理收货人地址代码不为空的情况
            String recipientWarehouseCode = uploadDTO.getRecipientWarehouseCode();
            if(Detect.notEmpty(recipientWarehouseCode)){
                SysWarehouse sysWarehouse = warehouseCodeMap.get(recipientWarehouseCode);
                if(sysWarehouse != null){
                    uploadDTO.setRecipientName(sysWarehouse.getContactName());
                    uploadDTO.setRecipientCompany(sysWarehouse.getCompanyName());
                    uploadDTO.setPhone(sysWarehouse.getContactPhone());
                    uploadDTO.setEmail(sysWarehouse.getContactEmail());
                    uploadDTO.setAddressLine1(sysWarehouse.getAddressLine1());
                    uploadDTO.setAddressLine2(sysWarehouse.getAddressLine2());
                    uploadDTO.setAddressLine3(sysWarehouse.getAddressLine3());
                    uploadDTO.setCity(sysWarehouse.getCity());
                    uploadDTO.setCountry(sysWarehouse.getCountry());
                    uploadDTO.setState(sysWarehouse.getState());
                    uploadDTO.setPostcode(sysWarehouse.getPostCode());
                }else{
                    errors.add(SubError.build(PclOrderB2cResultCode.SHIPPER_WAREHOUSE_CODE_NOT_NULL, uploadDTO.getRowNum()));
                }
            }
        }
        if(Detect.notEmpty(errorList)){
            errors.addAll(errorList);
            return null;
        }
        //遍历校验
        HashMap<String, List<SysAddressRuleConfig>> rulesCacheMap = Maps.newHashMap();
        for (PclOrderB2cUploadDTO uploadDTO : pclOrderB2cUploadDTOList){
            perfectRecipient(uploadDTO);
            //校验地址是否需要切换服务
            PclOrderB2cSwitchChannelDTO pclOrderB2cSwitchChannelDTO=BeanUtils.transform(uploadDTO,PclOrderB2cSwitchChannelDTO.class);
            PclOrderB2cSwitchChannelResultDTO switchChannelDTO = getSwitchChannelDTO(pclOrderB2cSwitchChannelDTO);
            if(switchChannelDTO.getServiceChanged()!=null&&switchChannelDTO.getServiceChanged()){
                uploadDTO.setVirtualChannelName(switchChannelDTO.getServiceName());
                uploadDTO.setServiceChanged(switchChannelDTO.getServiceChanged());
                uploadDTO.setVirtualChannelCode(switchChannelDTO.getServiceCode());
                uploadDTO.setChangeServiceBeforeCode(switchChannelDTO.getChangeServiceBeforeCode());
                uploadDTO.setChangeServiceBeforeName(switchChannelDTO.getChangeServiceBeforeName());
                uploadDTO.setAddressClassification(switchChannelDTO.getAddressClassification());
                log.info("订单：{}，服务进行更换，由：{}，变更为：{}",uploadDTO.getReferenceNo(),switchChannelDTO.getChangeServiceBeforeName(),switchChannelDTO.getServiceName());
            }
            if(Detect.notEmpty(switchChannelDTO.getAddressClassification())){
                uploadDTO.setAddressClassification(switchChannelDTO.getAddressClassification());
                uploadDTO.setServiceChanged(false);
            }
            String virtualChannelName = uploadDTO.getVirtualChannelName();
            String shipperWarehouseCode = uploadDTO.getShipperWarehouseCode();
            String key = virtualChannelName + " || " + shipperWarehouseCode;
            List<SysAddressRuleConfig> addressRuleConfigs = rulesCacheMap.get(key);
            if(!Detect.notEmpty(addressRuleConfigs)){
                SysOverseasWarehouseChannelDTO warehouseChannelDTO = sysOverseasWarehouseChannelDao.selectByWarehouseAndChannelName(shipperWarehouseCode, virtualChannelName, aggregator, channelType);
                if (warehouseChannelDTO != null) {
                    addressRuleConfigs = sysAddressRuleConfigService.getRuleConfigs(warehouseChannelDTO.getId());
                    rulesCacheMap.put(key, addressRuleConfigs);
                }
            }
            List<SubError> subErrors = sysAddressRuleConfigService.validateUploadAddressFields(uploadDTO, addressRuleConfigs);
            errors.addAll(subErrors);
        }
        if(Detect.notEmpty(errorList)){
            return null;
        }
        return rulesCacheMap;
    }

    @Override
    public void checkShipperWarehouseFrozen(List<PclOrderB2cThirdDTO> pclOrderB2cDTOList, List<SubError> errors) {
        //根据shipper + warehouseCode去重
        pclOrderB2cDTOList = pclOrderB2cDTOList.stream()
                .collect(Collectors.collectingAndThen(Collectors.toCollection(() ->
                        new TreeSet<>(Comparator.comparing(o -> o.getShipperWarehouseCode()))), ArrayList::new));
        // 校验海外仓账户是否被冻结
        pclOrderB2cDTOList.forEach(orderB2cDTO ->{
            Integer shipperId = orderB2cDTO.getShipperId();
            String shipperWarehouseCode = orderB2cDTO.getShipperWarehouseCode();
            if(shipperId == null){
                shipperId = SessionContext.getContext().getTenantId();
            }
            log.info("校验账户状态 shipperId:{}, warehouseCode:{} Start", shipperId, shipperWarehouseCode);
            List<SubError> errorList = actOverseasWarehouseAccountService.frozen(shipperId, shipperWarehouseCode);
            errors.addAll(errorList);
        });
    }

    private void transUploadInfo(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, List<PclOrderB2cThirdDTO> pclOrderB2cDTOList) {
        pclOrderB2cDTOList.forEach(pclOrderB2cDTO -> pclOrderB2cUploadDTOList.forEach(pclOrderB2cUploadDTO -> {
            if (pclOrderB2cDTO.getReferenceNo().equals(pclOrderB2cUploadDTO.getReferenceNo())) {
                pclOrderB2cUploadDTO.setOrderId(pclOrderB2cDTO.getId());
            }
        }));
    }

    private void createBatch(List<PclOrderB2cThirdDTO> pclOrderB2cDTOList, PartyVo partyVo) {
        List<PclBatch> pclBatchList = new ArrayList<>();
        pclOrderB2cDTOList.forEach(pclOrderB2cThirdDTO -> {
            List<B2cOrderProductDTO> orderItems = pclOrderB2cThirdDTO.getOrderItems();
            if (orderItems.size() > 0) {
                orderItems.stream().collect(Collectors.groupingBy(
                        B2cOrderProductDTO::getSku
                )).forEach((sku, productList) -> {
                    int skuCount = (int) (productList.stream().collect(Collectors.summarizingInt(B2cOrderProductDTO::getItemCount)).getSum());
                    PclBatch pclBatch = new PclBatch();
                    Date date = new Date();
                    String batchNo = date.getTime() + pclOrderB2cThirdDTO.getShipperWarehouseCode() + sku;
                    SysProductDTO productDTO = sysProductDao.findByShipperAndSku(pclOrderB2cThirdDTO.getShipperId(), sku, ProductType.OVERSEASWAREHOUSE.getType());
                    pclBatch.setSku(sku);
                    pclBatch.setReferenceNo(pclOrderB2cThirdDTO.getReferenceNo());
                    pclBatch.setOrderNo(pclOrderB2cThirdDTO.getOrderNo());
                    pclBatch.setSkuNameEn(productDTO.getSkuNameEn());
                    pclBatch.setSkuNameLocal(productDTO.getSkuNameCn());
                    pclBatch.setBatchNo(batchNo);
                    pclBatch.setShipperId(pclOrderB2cThirdDTO.getShipperId());
                    pclBatch.setShipperName(partyVo.getNameEn());
                    pclBatch.setForecastAmount(skuCount);
                    pclBatch.setOverseasWarehouseCode(pclOrderB2cThirdDTO.getShipperWarehouseCode());
                    pclBatch.setOrderId(pclOrderB2cThirdDTO.getId());
                    pclBatch.setStatusUpdateTime(date);
                    pclBatch.setStatus(BatchStatus.OUTBOUND_PENDING.getStatus());
                    pclBatch.setSource(BatchSource.AUTOMATIC_BY_ORDER.getSource());
                    pclBatchList.add(pclBatch);
                });
            }
        });
        if (Detect.notEmpty(pclBatchList)) {
            pclBatchDao.insertList(pclBatchList);
        }
    }

    public void createInventoryAgingLog(PclInventoryAging pclInventoryAging, Integer changeCount, Long orderId, Short inventoryType) {
        PclInventoryLog pclInventoryLog = new PclInventoryLog();
        pclInventoryLog.initCreateEntity();
        pclInventoryLog.setOrderId(orderId);
        pclInventoryLog.setInventoryAgingId(pclInventoryAging.getId());
        pclInventoryLog.setChangeType(InventoryChangeType.OUTBOUND.getType());
        pclInventoryLog.setChangeCount(changeCount);
        pclInventoryLog.setShipperId(pclInventoryAging.getShipperId());
        pclInventoryLog.setInventoryType(inventoryType);
        pclInventoryLogDao.insert(pclInventoryLog);
    }

    @Override
    public void printLabels(PclOrderB2cPrintLabelsRequest printLabelsRequest, HttpServletResponse response) {
        List<String> LabelContentList = new ArrayList<>();
        List<String> orderNos = printLabelsRequest.getOrderNos();
        if (Detect.notEmpty(orderNos)) {
            //调用接口打印面单
            String printLabelsUrl = b2cOrderProperties.getPrintLabelsUrl();
            //根据orderId获取所有order信息和对应的shipper信息
            List<B2cOrderAndShipperDTO> b2cOrderAndShipperDTOList = pclOrderB2cDao.selectOrderAndShipperInfoByOrderNos(orderNos);
            AtomicReference<Boolean> foreachBreak = new AtomicReference<>(false);
            //根据shipper进行分组
            b2cOrderAndShipperDTOList.stream().collect(Collectors.groupingBy(B2cOrderAndShipperDTO::getShipperId)
            ).forEach((shipperId, orderInfoList) -> {
                B2cOrderAndShipperDTO b2cOrderAndShipperDTO = orderInfoList.get(0);
                if (!Detect.notEmpty(b2cOrderAndShipperDTO.getAccessKey()) || !Detect.notEmpty(b2cOrderAndShipperDTO.getToken())) {
                    try {
                        foreachBreak.set(true);
                        log.info("调用打印面单接口失败:{}", "shipper:" + shipperId + "没有token和key");
                    } catch (Exception e) {
                        log.error("调用打印面单接口失败:{}", "shipper:" + shipperId + "没有token和key");
                    }
                }
                if (foreachBreak.get()) {
                    return;
                }
                List<String> orderNoList = orderInfoList.stream().map(B2cOrderAndShipperDTO::getOrderNo).collect(Collectors.toList());
                //获取打印面单的请求参数
                printLabelsRequest.setOrderNos(orderNoList);
                PclOrderB2cPrintLabelsThirdDTO pclOrderB2cPrintLabelsThirdDTO = getPrintLabelThirdInfo(printLabelsRequest);
                HttpHeaders httpHeaders = buildHttpHeadersForApi(b2cOrderAndShipperDTO.getAccessKey(), b2cOrderAndShipperDTO.getToken(), printLabelsUrl);
                try {
                    ResponseEntity<String> result = restTemplate.postForEntity(printLabelsUrl, new HttpEntity<>(pclOrderB2cPrintLabelsThirdDTO, httpHeaders), String.class);
                    //解析返回值结果
                    if (result.getStatusCode().equals(HttpStatus.OK)) {
                        JSONObject resultJsonObject = JSONObject.parseObject(result.getBody());
                        JSONArray resultErrors = resultJsonObject.getJSONArray("errors");
                        if (Detect.notEmpty(resultErrors)) {
                            for (int i = 0; i < resultErrors.size(); i++) {
                                JSONObject errorMessageJsonObject = resultErrors.getJSONObject(i);
                                try {
                                    response.setCharacterEncoding("UTF-8");
                                    response.setContentType("text/html;charset=utf-8");
                                    Writer writer = response.getWriter();
                                    writer.write(errorMessageJsonObject.getString("message"));
                                    writer.close();
                                } catch (Exception e) {
                                    log.error("调用打印面单接口失败:{}", "orderIds 为空");
                                }
                            }
                        } else {
                            JSONArray resultData = resultJsonObject.getJSONArray("data");
                            if (Detect.notEmpty(resultData)) {
                                for (int i = 0; i < resultData.size(); i++) {
                                    JSONObject createdOrderObject = resultData.getJSONObject(i);
                                    String labelContent = createdOrderObject.getString("labelContent");
                                    LabelContentList.add(labelContent);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("调用打印面单接口失败:{}", e.getMessage());
                }
            });
            if (Detect.notEmpty(LabelContentList)) {
                dealWithLabelBase64Info(LabelContentList, response);
            }
        } else {
            try {
                response.setCharacterEncoding("UTF-8");
                response.setContentType("text/html;charset=utf-8");
                Writer writer = response.getWriter();
                writer.write("orderIds not be null");
                writer.close();
            } catch (Exception e) {
                log.error("调用打印面单接口失败:{}", "orderIds 为空");
            }
        }
    }

    @Override
    public List<PclOrderB2cDTO> findOrderListByTrackingList(List<String> trackingNoList) {
        return pclOrderB2cDao.selectByTrackingNoList(trackingNoList);
    }

    /*@Override
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRED)
    public List<SubError> batchDespatch(List<String> orderIds) {
        List<SubError> errorList = new ArrayList<>();
        //根据orderIds 获取订单信息
        List<B2cOrderAndShipperDTO> b2cOrderAndShipperDTOS = pclOrderB2cDao.selectOrderAndShipperInfoByOrderIds(orderIds);
        b2cOrderAndShipperDTOS.forEach(b2cOrderAndShipperDTO -> {
            SysParty shipper = sysPartyService.getByPartyId(b2cOrderAndShipperDTO.getShipperId());
            //效验状态  已创建 或 下单异常 才能确认发送
            Short status = b2cOrderAndShipperDTO.getStatus();
            if (!(B2cOrderStatus.UNCONFIRMED.getType().equals(status) || B2cOrderStatus.SENDING_ABNORMALITY.getType().equals(status))) {
                errorList.add(SubError.build(PclOrderB2cResultCode.DESPATCH_STATUS_ERROR, b2cOrderAndShipperDTO.getOrderNo()));
            }
            //效验订单发送商品库存是否充足
            List<SubError> subErrorList = checkOrderInventory(b2cOrderAndShipperDTO.getOrderNo(), shipper);
            //更新订单的错误原因字段信息
            if(Detect.notEmpty(subErrorList)){
                PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(b2cOrderAndShipperDTO.getOrderId());
                List<String> messageList = subErrorList.stream().map(SubError::getMessage).collect(Collectors.toList());
                String messageStr= StringUtils.join(messageList, ";");
                if(messageStr.length()>230){
                    String substringMessage = messageStr.substring(0, 230);
                    pclOrderB2c.setAbnormalCauses(substringMessage);
                }else{
                    pclOrderB2c.setAbnormalCauses(messageStr);
                }
                //未通过eto系统验证 更新订单状态为「下单异常」
                pclOrderB2c.setStatus(B2cOrderStatus.SENDING_ABNORMALITY.getType());
                pclOrderB2c.initUpdateEntity();
                pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
                errorList.addAll(subErrorList);
            }
        });
        if (Detect.notEmpty(errorList)) {
            return errorList;
        }
        //获取没有serviceCode的订单
        List<B2cOrderAndShipperDTO> withoutTrackingNoList = b2cOrderAndShipperDTOS.stream()
                .filter(order -> !Detect.notEmpty(order.getTrackingNo()))
                .collect(Collectors.toList());
        List<String> orderNoList = new ArrayList<>();
        if (Detect.notEmpty(withoutTrackingNoList)) {
            orderNoList.addAll(withoutTrackingNoList.stream().map(B2cOrderAndShipperDTO::getOrderNo).collect(Collectors.toList()));
        }
        if(Detect.notEmpty(orderNoList)){
            //wms仓库出库 获取出库订单需要的参数
            List<WmsCreateOrderDetailDTO> wmsCreateOrderDetailDTOList = pclOrderB2cDao.selectOrderAndProductInfoForWmsOrder(orderNoList);
            //添加wms 出库订单 更新订单信息 和更新库存信息
            List<String> orderIdsWithoutServiceCode = new ArrayList<>();
            createOderAndUpdateInventory(wmsCreateOrderDetailDTOList, errorList, orderIdsWithoutServiceCode);
            if (errorList.size() == 0 && orderIdsWithoutServiceCode.size() > 0) {
                final List<SubError> errors = calculatorHandleFee(orderIdsWithoutServiceCode, true);
                if (errors != null && errors.size() > 0) {
                    errorList.addAll(errors);
                }
            } else {
                log.info("no need to cal handle fee");
            }
        }
        return errorList;
    }*/

    @Override
    public List<SubError> batchDespatch(List<String> orderIds) {
        List<SubError> errorList = new ArrayList<>();
        //根据orderIds 获取订单信息
        List<B2cOrderAndShipperDTO> b2cOrderAndShipperDTOS = pclOrderB2cDao.selectOrderAndShipperInfoByOrderIds(orderIds);
        //校验
        checkDespatchData(b2cOrderAndShipperDTOS, errorList);
        if(Detect.notEmpty(errorList)){
            return errorList;
        }
        List<String> orderNoList = b2cOrderAndShipperDTOS.stream().map(B2cOrderAndShipperDTO::getOrderNo).collect(Collectors.toList());
        //wms仓库出库 获取出库订单需要的参数
        List<WmsCreateOrderDetailDTO> wmsCreateOrderDetailDTOList = pclOrderB2cDao.selectOrderAndProductInfoForWmsOrder(orderNoList);
        //按海外仓代码对接平台分组,未对接得为null platform为0
        Map<Integer, List<WmsCreateOrderDetailDTO>> groupPlatformMap = wmsCreateOrderDetailDTOList.stream().collect(Collectors.groupingBy(x -> Optional.ofNullable(x.getWarehousePlatform()).orElse(0)));
        for (Integer platform : groupPlatformMap.keySet()) {
            List<WmsCreateOrderDetailDTO> wmsCreateOrderDTOLGroupPlatformList = groupPlatformMap.get(platform);
            wmsCreateOrderDTOLGroupPlatformList.stream().collect(Collectors.groupingBy(WmsCreateOrderDetailDTO::getOrderNo))
                    .forEach((orderNo, createWmsOrderDetailList) -> {
                        //>>>>>>lock stock
                        log.info("确认发送-订单号：{},库存加锁 Start", orderNo);
                        List<String> skuList = createWmsOrderDetailList.stream().map(p -> p.getSku()).distinct().collect(toList());
                        WmsCreateOrderDetailDTO orderDetailSampleDTO = createWmsOrderDetailList.get(0);
                        List<PclInventory> inventoryList = pclInventoryDao.queryBySkuAndOverseasWarehouseCodeAndShipper(skuList, orderDetailSampleDTO.getShipperWarehouseCode(), orderDetailSampleDTO.getShipperId(), SessionContext.getContext().getAggregator());
                        RLock multiLock = null;
                        try {
                           List<RLock> rLockList = inventoryList.stream().map(inventory -> {
                                return redissonService.getRLock("invId:" + inventory.getId());
                           }).collect(toList());
                           RLock[] arrayLock = rLockList.stream().toArray(RLock[]::new);
                           multiLock = redissonService.getMultiLock(arrayLock);
                           boolean locked = multiLock.tryLock(1, TimeUnit.SECONDS);
                           if (!locked) {
                               log.warn("确认发送-订单号：{},库存加锁 Failed", orderNo);
                               errorList.add(SubError.build(PclOrderB2cResultCode.ORDER_SKU_INVENTORY_BE_OCCUPIED, orderNo));
                               return;
                           }
                       } catch (Exception e) {
                           log.error("库存加锁失败:{}", ExUtil.getStackTrace(e));
                           return;
                       }
                        //>>>>>>main logic
                        try {
                            log.info("确认发送-订单号：{},库存加锁 Success", orderNo);
                            //校验库存
                            PartyVo partyVo = sysPartyService.getPartyVoByPartyId(orderDetailSampleDTO.getShipperId());
                            List<SubError> inventoryErrors = checkOrderInventory(orderNo, partyVo);
                            if (Detect.notEmpty(inventoryErrors)) {
                                log.info("确认发送-订单号：{},库存不足...", orderNo);
                                errorList.addAll(inventoryErrors);
                                PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(orderDetailSampleDTO.getId());
                                updateOrderSendAbnormality(pclOrderB2c, inventoryErrors);
                                return;
                            }
                            //添加wms 出库订单 更新订单信息 和更新库存信息(发送一个订单到海外仓)
                            PclOrderB2cDespachResultDTO despach = pclOrderB2cService.despach(createWmsOrderDetailList, true);
                            if(Detect.notEmpty(despach.getErrors())){
                                errorList.addAll(despach.getErrors());
                            }
                        } finally {
                            // TODO: 2024/8/7 解锁
//                            if (multiLock.isLocked() && multiLock.isHeldByCurrentThread()){
                                multiLock.unlock();
                                log.info("确认发送-释放库存锁 Success");
//                            }
                        }
                    });
        }
        return errorList;
    }

    @Override
    public void updateDespachInventoryInfo(WmsCreateOrderResponseDTO wmsCreateOrderResponseDTO, List<SubError> errorList) {
        log.info("createOrderResponse:{}", JSONObject.toJSONString(wmsCreateOrderResponseDTO));
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();
        if ("Success".equals(wmsCreateOrderResponseDTO.getAsk())) {
            List<SubError>orderError=new ArrayList<>();
            List<PclOrderB2cDTO> pclOrderB2cDTOList = pclOrderB2cDao.findOrderAndProductByOrderNo(wmsCreateOrderResponseDTO.getOrderNo());
            PclOrderB2cDTO pclOrderB2cDTO = pclOrderB2cDTOList.get(0);
            Integer shipperId = pclOrderB2cDTO.getShipperId();
            SysParty shipper = sysPartyService.getByPartyId(shipperId);
            PclOrderB2c pclOrderB2c = new PclOrderB2c();
            pclOrderB2c.setId(pclOrderB2cDTO.getId());
            //设置提交时间
            pclOrderB2c.setDateSending(new Date());
            pclOrderB2c.setThirdOrderNo(wmsCreateOrderResponseDTO.getOrderCode());
            pclOrderB2c.setTrackingNo(wmsCreateOrderResponseDTO.getTrackingNo());
            log.info("create b2cOrder batch:{}", JSONObject.toJSONString(pclOrderB2cDTOList));
            //更新待出库库存
            updateOutGoingInventory(pclOrderB2cDTOList, shipper, orderError);
            if(!Detect.notEmpty(orderError)){
                pclOrderB2c.initUpdateEntity();
                pclOrderB2c.setStatus(B2cOrderStatus.CONFIRMED_OUTBOUND.getType());
                //恢复异常原因为空
                pclOrderB2c.setAbnormalCauses("");
                pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
                //生成待出库批次
                createBatch(pclOrderB2cDTOList, shipper);
                // 计算 海外仓 处理费
                calculatorHandleFee(Arrays.asList(wmsCreateOrderResponseDTO.getOrderNo()), true);
                PclOrderB2c pclOrderB2c1 = pclOrderB2cDao.selectByPrimaryKey(pclOrderB2c.getId());
                orderEventService.packageEvents(pclOrderB2c1.getOrderNo(),pclOrderB2c1.getReferenceNo(),pclOrderB2c1.getId(),events, WareHouseOrderEventCode.ORDER_ACCEPTED);
            }else{
                List<String> messageList = orderError.stream().map(SubError::getMessage).collect(Collectors.toList());
                String messageStr= StringUtils.join(messageList, ";");
                if(messageStr.length()>230){
                    String substringMessage = messageStr.substring(0, 230);
                    pclOrderB2c.setAbnormalCauses(substringMessage);
                }else{
                    pclOrderB2c.setAbnormalCauses(messageStr);
                }
                pclOrderB2c.initUpdateEntity();
                pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
                errorList.addAll(orderError);
            }
        } else {
            PclOrderB2c pclOrderB2c = pclOrderB2cDao.findByOrderNo(wmsCreateOrderResponseDTO.getOrderNo());
            PclOrderB2c updatePclOrderB2c = new PclOrderB2c();
            updatePclOrderB2c.setId(pclOrderB2c.getId());
            updatePclOrderB2c.initUpdateEntity();
            updatePclOrderB2c.setAbnormalCauses(com.walltech.common.utils.StringUtils.subStringNull(wmsCreateOrderResponseDTO.getErrMessage(), 2000));
            updatePclOrderB2c.setTrackingNo(wmsCreateOrderResponseDTO.getTrackingNo());
            //海外仓下单失败 更新订单状态为「下单异常」
            updatePclOrderB2c.setStatus(B2cOrderStatus.SENDING_ABNORMALITY.getType());
            pclOrderB2cDao.updateByPrimaryKeySelective(updatePclOrderB2c);
            errorList.add(SubError.build(PclOrderB2cResultCode.DESPATCH_OUTBOUND_ERROR,wmsCreateOrderResponseDTO.getOrderNo(), wmsCreateOrderResponseDTO.getErrMessage()));
            orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.SENDING_ABNORMALITY);
        }
        orderEventService.addEventsByApiNoRepetition(events);

    }

    /**
     * 校验确认发送订单数据
     * @param b2cOrderAndShipperDTOS
     * @param errorList
     */
    private void checkDespatchData(List<B2cOrderAndShipperDTO> b2cOrderAndShipperDTOS, List<SubError> errorList) {
        //效验状态  已创建 或 下单异常 才能确认发送
        b2cOrderAndShipperDTOS.forEach(b2cOrderAndShipperDTO ->{
            Short status = b2cOrderAndShipperDTO.getStatus();
            if (!(B2cOrderStatus.UNCONFIRMED.getType().equals(status) || B2cOrderStatus.SENDING_ABNORMALITY.getType().equals(status))) {
                errorList.add(SubError.build(PclOrderB2cResultCode.DESPATCH_STATUS_ERROR, b2cOrderAndShipperDTO.getOrderNo()));
            }
        });
        if (Detect.notEmpty(errorList)) {
            return;
        }
        //效验订单发送商品库存是否充足
        Map<Integer, PartyVo> partyVoMap = sysPartyService.findShipperMapByAggregator(SessionContext.getContext().getAggregator());
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();

        b2cOrderAndShipperDTOS.forEach(b2cOrderAndShipperDTO ->{
            PartyVo partyVo = partyVoMap.get(b2cOrderAndShipperDTO.getShipperId());
            PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(b2cOrderAndShipperDTO.getOrderId());
            //校验订单关联的服务是否已经失效
            if (pclOrderB2c != null && pclOrderB2c.getChannelId() != null) {
//                SysOverseasWarehouseChannel warehouseChannel = sysOverseasWarehouseChannelDao.selectByPrimaryKey(pclOrderB2c.getChannelId());
//                if (Objects.equals(ActiveType.INVALID.getType(), warehouseChannel.getStatus())) {
//                    errorList.add(SubError.build(PclOrderB2cResultCode.B2C_ORDER_WAREHOUSE_CHANNEL_INACTIVE_ERROR, pclOrderB2c.getReferenceNo(), warehouseChannel.getVirtualChannelName()));
//                    return;
//                }
                List<SysOverseasWarehouseChannelSimpleDTO> invalidWarehouseChannelList = sysOverseasWarehouseChannelDao.getByWarehouseCodeAndShipperChannelActive(
                        b2cOrderAndShipperDTO.getShipperId(),
                        b2cOrderAndShipperDTO.getShipperWarehouseCode(),
                        ChannelType.FULFILMENT.getType(), ActiveTypeNew.INVALID.getType());
                SysOverseasWarehouseChannelSimpleDTO warehouseChannel = invalidWarehouseChannelList.stream().filter(p -> Objects.equals(p.getId(), pclOrderB2c.getChannelId())).findFirst().orElse(null);
                if (warehouseChannel != null) {
                    errorList.add(SubError.build(PclOrderB2cResultCode.B2C_ORDER_WAREHOUSE_CHANNEL_INACTIVE_ERROR, pclOrderB2c.getReferenceNo(), warehouseChannel.getVirtualChannelName()));
                    return;
                }
                //元数据校验 包裹尺寸
                List<PclOrderB2cDetailItemVO> pclProductList = pclProductB2cDao.findDetailInfoByOrderId(pclOrderB2c.getId());
                List<String> thirdSkuNoList = pclProductList.stream().map(PclOrderB2cDetailItemVO::getThirdSkuNo).collect(Collectors.toList());
                List<SysProduct> sysProductList = sysProductDao.findByThirdSkuAndShipperList(thirdSkuNoList,pclOrderB2c.getShipperId());
                List<SysProductCheckDTO> sysCheckProductList = new ArrayList<>();
                if(CollectionUtils.isNotEmpty(sysProductList)){
                    Map<String, Long> skuMap =pclProductList.stream()
                            .collect(Collectors.toMap(PclOrderB2cDetailItemVO::getSku, PclOrderB2cDetailItemVO::getProductCount, Long::sum));
                    sysProductList.forEach(e ->{
                        e.setGoodsQty(skuMap.get(e.getSku()).intValue());
                    });
                    sysCheckProductList = BeanUtils.transform(sysProductList, SysProductCheckDTO.class);
                }
                List<SysAddressRuleConfig> addressRuleConfigList = sysAddressRuleConfigDao.getActiveByChannelId(pclOrderB2c.getChannelId());
                List<SubError> errors = sysAddressRuleConfigService.validateUploadPacketWeight(sysCheckProductList, addressRuleConfigList);
                if(CollectionUtils.isNotEmpty(errors)){
                    errorList.addAll(errors);
                    return;
                }
            }
            List<SubError> subErrorList = checkOrderInventory(b2cOrderAndShipperDTO.getOrderNo(), partyVo);
            updateOrderSendAbnormality(pclOrderB2c, subErrorList);
            errorList.addAll(subErrorList);
        });
    }

    private void updateOrderSendAbnormality(PclOrderB2c pclOrderB2c, List<SubError> subErrorList) {
        //更新订单的错误原因字段信息
        if(Detect.notEmpty(subErrorList)){
            PclOrderB2c updatePclOrderB2c = new PclOrderB2c();
            updatePclOrderB2c.setId(Long.valueOf(pclOrderB2c.getId()));
            List<String> messageList = subErrorList.stream().map(SubError::getMessage).collect(Collectors.toList());
            String messageStr= StringUtils.join(messageList, ";");
            if(messageStr.length()>230){
                String substringMessage = messageStr.substring(0, 230);
                updatePclOrderB2c.setAbnormalCauses(substringMessage);
            }else{
                updatePclOrderB2c.setAbnormalCauses(messageStr);
            }
            //未通过eto系统验证 更新订单状态为「下单异常」
            updatePclOrderB2c.setStatus(B2cOrderStatus.SENDING_ABNORMALITY.getType());
            updatePclOrderB2c.initUpdateEntity();
            pclOrderB2cDao.updateByPrimaryKeySelective(updatePclOrderB2c);
            List<TrkSourceOrderEventDTO> events = Lists.newArrayList();
            orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.SENDING_ABNORMALITY);
            orderEventService.addEventsByApiNoRepetition(events);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRES_NEW)
    public PclOrderB2cDespachResultDTO despach(List<WmsCreateOrderDetailDTO> createWmsOrderDetailList,Boolean updateInventory) {
        PclOrderB2cDespachResultDTO result = new PclOrderB2cDespachResultDTO();
        List<SubError> errors = Lists.newArrayList();
        Long orderId;
        Integer platform = createWmsOrderDetailList.get(0).getWarehousePlatform();
        Integer shipperId = createWmsOrderDetailList.get(0).getShipperId();
        //如果 是优先下单订单，则需先向B2c平台下单
        Boolean isPriorityOrder = createWmsOrderDetailList.get(0).getIsPriorityOrder();
        WmsCreateOrderResponseDTO wmsCreateOrderResponseDTO =new WmsCreateOrderResponseDTO();
        B2cCreateOrderResponseDTO b2cCreateOrderResponseDTO=new B2cCreateOrderResponseDTO();
        PartyVo shipperVo = sysPartyService.findByPartyId(shipperId);
        if(Objects.equals(isPriorityOrder,true)){
            b2cCreateOrderResponseDTO = createLastMileOrder(createWmsOrderDetailList);
        }
        String orderNo = createWmsOrderDetailList.get(0).getOrderNo();
        PclOrderB2c newPclOrderB2c = pclOrderB2cDao.findByOrderNo(orderNo);
        orderId=newPclOrderB2c.getId();
        //b2c订单创建失败则存储错误原因
        if(Detect.notEmpty(b2cCreateOrderResponseDTO.getErrorMessage())){
            wmsCreateOrderResponseDTO.setOrderNo(orderNo);
            wmsCreateOrderResponseDTO.setAsk(ResponseStatus.FAILED.getName());
            wmsCreateOrderResponseDTO.setErrMessage(b2cCreateOrderResponseDTO.getErrorMessage());
        }else{
            //b2c创建成功，继续海外仓确认发送流程
            if (platform==null||platform.equals(0)) {
                wmsCreateOrderResponseDTO.setAsk(ResponseStatus.SUCCESS.getName());
                wmsCreateOrderResponseDTO.setOrderNo(orderNo);
            } else {
                //对接仓
                WmsCreateOrderDetailDTO wmsCreateOrderDetailDTO = createWmsOrderDetailList.get(0);
                wmsCreateOrderDetailDTO.setOrderNo(orderNo);
                WmsCreateOrderDTO wmsCreateOrderDTO = BeanUtils.transform(wmsCreateOrderDetailDTO, WmsCreateOrderDTO.class);
                wmsCreateOrderDTO.setShipperWarehouseCode(Detect.notEmpty(wmsCreateOrderDetailDTO.getThirdWarehouseCode()) ? wmsCreateOrderDetailDTO.getThirdWarehouseCode() : wmsCreateOrderDetailDTO.getShipperWarehouseCode());
                List<WmsItemForCreateOrderDTO> orderItems = createWmsOrderDetailList.stream().map(orderDetail -> {
                    WmsItemForCreateOrderDTO wmsItemForCreateOrderDTO = BeanUtils.transform(orderDetail, WmsItemForCreateOrderDTO.class);
                    if(!(platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode()) || platform.equals(OverseasWarehousePlatform.WMS_UBAY.getPlatformCode()))){
                        wmsItemForCreateOrderDTO.setSku(orderDetail.getThirdSkuNo());
                    }else{
                        wmsItemForCreateOrderDTO.setSku(orderDetail.getSku());
                    }
                    wmsItemForCreateOrderDTO.setSysSku(orderDetail.getSku());
                    return wmsItemForCreateOrderDTO;
                }).collect(Collectors.toList());
                //处理地址栏信息 放到实现类中处理
                /*if(!platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode())){
                    String consigneeAddress =(Detect.notEmpty(wmsCreateOrderDTO.getConsigneeAddress1()) ? wmsCreateOrderDTO.getConsigneeAddress1() : "")
                            + (Detect.notEmpty(wmsCreateOrderDTO.getConsigneeAddress2()) ? wmsCreateOrderDTO.getConsigneeAddress2() : "")
                            + (Detect.notEmpty(wmsCreateOrderDTO.getConsigneeAddress3()) ? wmsCreateOrderDTO.getConsigneeAddress3() : "");
                    if(consigneeAddress.length() > 50){
                        wmsCreateOrderDTO.setConsigneeAddress2(consigneeAddress.substring(50));
                        wmsCreateOrderDTO.setConsigneeAddress1(consigneeAddress.substring(0,50));
                    }
                }else{
                    handleMaerskAddress(wmsCreateOrderDTO);
                }*/
                wmsCreateOrderDTO.setItems(orderItems);
                if(!platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode())){
                    wmsCreateOrderDTO.setPlatform("OTHER");
                    wmsCreateOrderDTO.setShippingMethod(wmsCreateOrderDetailDTO.getVirtualChannelCode());
                    wmsCreateOrderDTO.setIsShippingMethodNotAllowUpdate(1);
                    wmsCreateOrderDTO.setIsChangeLabel(0);
                    wmsCreateOrderDTO.setVerify(1);
                }
                //面单信息
                if(isPriorityOrder){
                    List<PclOrderB2cThirdDTO> pclOrderB2cDTOList = b2cCreateOrderResponseDTO.getPclOrderB2cDTOList();
                    PclOrderB2cThirdDTO pclOrderB2cThirdDTO = pclOrderB2cDTOList.get(0);
                    WmsCreateOrderLabelDTO wmsCreateOrderLabelDTO=new WmsCreateOrderLabelDTO();
                    wmsCreateOrderLabelDTO.setFileData(pclOrderB2cThirdDTO.getLabelContent());
                    wmsCreateOrderDTO.setLabelInfo(wmsCreateOrderLabelDTO);
                    wmsCreateOrderDTO.setTrackingNo(pclOrderB2cThirdDTO.getTrackingNo());
                }
                //处理文件
                List<PclOrderB2cAttachment> pclOrderB2cAttachments = pclOrderB2cAttachmentDao.selectByOrderId(orderId);
                List<WmsAttachForCreateOrderDTO> wmsAttachForCreateOrderDTOList = pclOrderB2cAttachments.stream()
                        .filter(attachFile->Detect.notEmpty(attachFile.getThirdFileId())).map(attachFile -> {
                    String fileType = attachFile.getFileName().substring(attachFile.getFileName().lastIndexOf(".") + 1);
                    WmsAttachForCreateOrderDTO wmsAttachForCreateOrderDTO = new WmsAttachForCreateOrderDTO();
                    wmsAttachForCreateOrderDTO.setAttachId(attachFile.getThirdFileId());
                    wmsAttachForCreateOrderDTO.setFileType(fileType);
                    return wmsAttachForCreateOrderDTO;
                }).collect(Collectors.toList());
                if(Detect.notEmpty(wmsAttachForCreateOrderDTOList)){
                    wmsCreateOrderDTO.setAttach(wmsAttachForCreateOrderDTOList);
                }
                wmsCreateOrderDTO.setSysAttaches(pclOrderB2cAttachments);
                WmsHandleStrategyService shippingPlanStrategyService = wmsHandleServiceMap.get(platform);
                if (shippingPlanStrategyService != null) {
                    String remark = wmsCreateOrderDTO.getRemark();
                    wmsCreateOrderResponseDTO = shippingPlanStrategyService.createOrder(wmsCreateOrderDTO);
                    if (!Objects.equals(remark, wmsCreateOrderDTO.getRemark())) {
                        log.info("更新备注");
                        PclOrderB2c pclOrderB2c = new PclOrderB2c();
                        pclOrderB2c.setId(wmsCreateOrderDetailDTO.getId());
                        pclOrderB2c.setRemark(wmsCreateOrderDTO.getRemark());
                        pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
                    }
                } else {
                    log.error("Method [{}],platform id [{}] can not match any handle service, please check...", "createOrder", platform);
                }
            }
        }
        if(isPriorityOrder&&!Detect.notEmpty(b2cCreateOrderResponseDTO.getErrorMessage())){
            //保存 b2c订单创建成功后返回的trackingNo
            List<PclOrderB2cThirdDTO> pclOrderB2cDTOList = b2cCreateOrderResponseDTO.getPclOrderB2cDTOList();
            PclOrderB2cThirdDTO pclOrderB2cThirdDTO = pclOrderB2cDTOList.get(0);
            wmsCreateOrderResponseDTO.setTrackingNo(pclOrderB2cThirdDTO.getTrackingNo());
            //保存  b2c订单创建成功后返回的面单
            PclOrderB2cLabel pclOrderB2cLabel=new PclOrderB2cLabel();
            pclOrderB2cLabel.initCreateEntity();
            pclOrderB2cLabel.setOrderId(orderId);
            pclOrderB2cLabel.setLabelContent(pclOrderB2cThirdDTO.getLabelContent());
            pclOrderB2cLabelDao.insertSelective(pclOrderB2cLabel);
            //预报b2c订单
            if(wmsCreateOrderResponseDTO.getAsk().equalsIgnoreCase(ResponseStatus.SUCCESS.getName())){
                List<String>trackingList= Collections.singletonList(pclOrderB2cThirdDTO.getTrackingNo());
                String closeRequest = JSON.toJSONString(concatCloseParam(trackingList));
                eTowerOrderService.closeB2cPacketOrder(b2cOrderProperties.getCloseShipmentsUrl(), closeRequest, shipperVo);
            }
        }
        if(Objects.equals(updateInventory,Boolean.TRUE)){
            //更新库存相关信息 并生成费用
            updateDespachInventoryInfo(wmsCreateOrderResponseDTO, errors);
        }else{
            if(!"Success".equals(wmsCreateOrderResponseDTO.getAsk())){
                errors.add(SubError.build(PclOrderB2cResultCode.DESPATCH_OUTBOUND_ERROR,wmsCreateOrderResponseDTO.getOrderNo(), wmsCreateOrderResponseDTO.getErrMessage()));
            }
        }
        result.setErrors(errors);
        result.setWmsCreateOrderResponseDTO(wmsCreateOrderResponseDTO);
        return result;
    }

    /**
     * 组装易仓标签传参
     *单标签格式： "label":{"file_type":"png","file_data":"XXX","file_size":"XXX","file_name":"name"},
     * 多标签格式："label":{"file_type":"png","file_size":"XXX","file_data":["data1","data2"],"file_name":["name1","name2"]}
     * @param pclOrderB2cAttachments
     * @return
     */
    private WmsCreateOrderLabelDTO buildLabelAttaches(List<PclOrderB2cAttachment> pclOrderB2cAttachments) {
        List<PclOrderB2cAttachment> labelAttachList = pclOrderB2cAttachments.stream()
                .filter(p -> Objects.equals(p.getFileType(), OrderAttachmentType.LABEL.getType())).collect(Collectors.toList());
        if (Detect.empty(labelAttachList)) {
            return null;
        }

        WmsCreateOrderLabelDTO wmsLabelAttach = new WmsCreateOrderLabelDTO();
        if (labelAttachList.size() == 1) {
            //一个附件
            PclOrderB2cAttachment attachment = labelAttachList.get(0);
            String fileName = attachment.getFileName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("."));
            wmsLabelAttach.setFileData(AWSFileUtils.getBase64ByFileId(attachment.getFileId()));
            wmsLabelAttach.setFileName(prefix);
        } else {
            //多个附件
            List<String> fileDatas = Lists.newArrayList();
            List<String> fileNames = Lists.newArrayList();
            labelAttachList.forEach(labelFile ->{
                fileDatas.add(AWSFileUtils.getBase64ByFileId(labelFile.getFileId()));
                String fileName = labelFile.getFileName();
                String prefix = fileName.substring(0, fileName.lastIndexOf("."));
                fileNames.add(prefix);
            });
            wmsLabelAttach.setFileData(fileDatas);
            wmsLabelAttach.setFileName(fileNames);
        }
        //设置标签类型 多个标签文件类型应该一致
        String fileName = labelAttachList.get(0).getFileName();
        String suffix = fileName.substring(fileName.lastIndexOf(".") + 1);
        wmsLabelAttach.setFileType(suffix);
        return wmsLabelAttach;
    }

    /**
     * 销售订单专用的履约接口
     * @param createWmsOrderDetailList
     * @param updateInventory
     * @return
     */
    @Override
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRES_NEW)
    public PclOrderB2cDespachResultDTO despachUseSalesOrder(List<WmsCreateOrderDetailDTO> createWmsOrderDetailList,Boolean updateInventory) {
        PclOrderB2cDespachResultDTO result = new PclOrderB2cDespachResultDTO();
        List<SubError> errors = Lists.newArrayList();
        String orderNo = createWmsOrderDetailList.get(0).getOrderNo();
        Integer platform = createWmsOrderDetailList.get(0).getWarehousePlatform();
        WmsCreateOrderResponseDTO wmsCreateOrderResponseDTO = new WmsCreateOrderResponseDTO();
        if (platform==null||platform.equals(0)) {
            wmsCreateOrderResponseDTO.setAsk("Success");
            wmsCreateOrderResponseDTO.setOrderNo(orderNo);
        } else {
            //对接仓
            WmsCreateOrderDetailDTO wmsCreateOrderDetailDTO = createWmsOrderDetailList.get(0);
            WmsCreateOrderDTO wmsCreateOrderDTO = BeanUtils.transform(wmsCreateOrderDetailDTO, WmsCreateOrderDTO.class);
            wmsCreateOrderDTO.setShipperWarehouseCode(Detect.notEmpty(wmsCreateOrderDetailDTO.getThirdWarehouseCode()) ? wmsCreateOrderDetailDTO.getThirdWarehouseCode() : wmsCreateOrderDetailDTO.getShipperWarehouseCode());
            List<WmsItemForCreateOrderDTO> orderItems = createWmsOrderDetailList.stream().map(orderDetail -> {
                WmsItemForCreateOrderDTO wmsItemForCreateOrderDTO = BeanUtils.transform(orderDetail, WmsItemForCreateOrderDTO.class);
                if(!(platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode()) || platform.equals(OverseasWarehousePlatform.WMS_UBAY.getPlatformCode()))){
                    wmsItemForCreateOrderDTO.setSku(orderDetail.getThirdSkuNo());
                }else{
                    wmsItemForCreateOrderDTO.setSku(orderDetail.getSku());
                }
                return wmsItemForCreateOrderDTO;
            }).collect(Collectors.toList());
            //处理地址栏信息  放到实现类中处理
            /*if(!platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode())){
                String consigneeAddress =(Detect.notEmpty(wmsCreateOrderDTO.getConsigneeAddress1()) ? wmsCreateOrderDTO.getConsigneeAddress1() : "")
                        + (Detect.notEmpty(wmsCreateOrderDTO.getConsigneeAddress2()) ? wmsCreateOrderDTO.getConsigneeAddress2() : "")
                        +(Detect.notEmpty(wmsCreateOrderDTO.getConsigneeAddress3()) ? wmsCreateOrderDTO.getConsigneeAddress3() : "");
                if(consigneeAddress.length() > 50){
                    wmsCreateOrderDTO.setConsigneeAddress2(consigneeAddress.substring(50));
                    wmsCreateOrderDTO.setConsigneeAddress1(consigneeAddress.substring(0,50));
                }
            }else{
                handleMaerskAddress(wmsCreateOrderDTO);
            }*/
            wmsCreateOrderDTO.setItems(orderItems);
            if(!platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode())){
                wmsCreateOrderDTO.setPlatform("OTHER");
                wmsCreateOrderDTO.setShippingMethod(wmsCreateOrderDetailDTO.getVirtualChannelCode());
                wmsCreateOrderDTO.setIsShippingMethodNotAllowUpdate(1);
                wmsCreateOrderDTO.setIsChangeLabel(0);
                wmsCreateOrderDTO.setVerify(1);
            }
            WmsHandleStrategyService shippingPlanStrategyService = wmsHandleServiceMap.get(platform);
            if (shippingPlanStrategyService != null) {
                wmsCreateOrderResponseDTO = shippingPlanStrategyService.createOrder(wmsCreateOrderDTO);
            } else {
                log.error("Method [{}],platform id [{}] can not match any handle service, please check...", "createOrder", platform);
            }
        }
        if(Objects.equals(updateInventory,Boolean.TRUE)){
            //更新库存相关信息 并生成费用
            updateDespachInventoryInfo(wmsCreateOrderResponseDTO, errors);
        }else{
            if(!"Success".equals(wmsCreateOrderResponseDTO.getAsk())){
                errors.add(SubError.build(PclOrderB2cResultCode.DESPATCH_OUTBOUND_ERROR,wmsCreateOrderResponseDTO.getOrderNo(), wmsCreateOrderResponseDTO.getErrMessage()));
            }
        }
        result.setErrors(errors);
        result.setWmsCreateOrderResponseDTO(wmsCreateOrderResponseDTO);
        return result;
    }

    private OrderCloseRequestVO concatCloseParam(List<String> trackingNos) {
        OrderCloseRequestVO closeRequestVO = new OrderCloseRequestVO();
        closeRequestVO.setShipmentIds(trackingNos);
        return closeRequestVO;
    }

    B2cCreateOrderResponseDTO createLastMileOrder(List<WmsCreateOrderDetailDTO> createWmsOrderDetailList) {
        WmsCreateOrderDetailDTO wmsCreateOrderDetailDTO = createWmsOrderDetailList.get(0);
        SysParty shipper = sysPartyService.getByPartyId(wmsCreateOrderDetailDTO.getShipperId());
        //转换创建b2c订单需要的参数
        PclOrderB2cThirdDTO pclOrderB2cRequestDTO=convertCreatePclOrderB2cThirdDTO(createWmsOrderDetailList);
        //如果是已创建过b2c订单 并且状态为下单异常的，则新创建一个订单，并重新调用接口创建b2c订单
        if(Detect.notEmpty(wmsCreateOrderDetailDTO.getTrackingNo())&&wmsCreateOrderDetailDTO.getStatus().equals(B2cOrderStatus.SENDING_ABNORMALITY.getType())){
            PclOrderB2c newOrder=createNewB2cOrderAndAbandonedOldOrder(wmsCreateOrderDetailDTO);
            pclOrderB2cRequestDTO.setOrderNo(newOrder.getOrderNo());
            pclOrderB2cRequestDTO.setReferenceNo(newOrder.getOrderNo());
            pclOrderB2cRequestDTO.setTrackingNo(newOrder.getTrackingNo());
            wmsCreateOrderDetailDTO.setOrderNo(newOrder.getOrderNo());
            wmsCreateOrderDetailDTO.setTrackingNo(newOrder.getTrackingNo());
        }
        return b2cShipperService.createB2cOrderList(Collections.singletonList(pclOrderB2cRequestDTO), shipper);
    }

    /**
     * 废弃旧订单，创建新订单
     * @param oldB2cOrder
     * @return newOrder
     */
    private PclOrderB2c createNewB2cOrderAndAbandonedOldOrder(WmsCreateOrderDetailDTO oldB2cOrder) {
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();
        PclOrderB2c oldOrder=pclOrderB2cDao.selectByPrimaryKey(oldB2cOrder.getId());
        //创建新订单
        PclOrderB2c newOrder= BeanUtils.transform(oldOrder,PclOrderB2c.class);
        newOrder.setId(IdGenerator.getInstance().generate());
        newOrder.setTrackingNo(StringUtils.EMPTY);
        newOrder.setOrderNo(BusinessNo.generator().partyId(oldOrder.getShipperId()).businessNoType(BusinessNoType.B2C_ORDER_NO).generate());
        newOrder.setStatus(B2cOrderStatus.UNCONFIRMED.getType());
        pclOrderB2cDao.insert(newOrder);
        orderEventService.packageEvents(newOrder.getOrderNo(),newOrder.getReferenceNo(),newOrder.getId(),events, WareHouseOrderEventCode.ORDER_CREATED);
        //废弃订单
        oldOrder.setStatus(B2cOrderStatus.ABANDONED.getType());
        oldOrder.initUpdateEntity();
        pclOrderB2cDao.updateByPrimaryKeySelective(oldOrder);
        orderEventService.packageEvents(oldOrder.getOrderNo(),oldOrder.getReferenceNo(),oldOrder.getId(),events, WareHouseOrderEventCode.ABANDONED);
        //获取
        List<PclProductB2c> oldOrderProductList= pclProductB2cDao.findByOrderId(oldB2cOrder.getId());
        List<PclProductB2c> newProductList = oldOrderProductList.stream().map(productB2c -> {
            PclProductB2c newProduct = BeanUtils.transform(productB2c, PclProductB2c.class);
            newProduct.setId(IdGenerator.getInstance().generate());
            newProduct.setOrderId(newOrder.getId());
            newProduct.initCreateEntity();
            return newProduct;
        }).collect(Collectors.toList());
        pclProductB2cDao.insertList(newProductList);
        return newOrder;
    }

    private PclOrderB2cThirdDTO convertCreatePclOrderB2cThirdDTO(List<WmsCreateOrderDetailDTO> createWmsOrderDetailList) {
        WmsCreateOrderDetailDTO wmsCreateOrderDetailDTO = createWmsOrderDetailList.get(0);
        PclOrderB2cThirdDTO pclOrderB2cThirdDTO=BeanUtils.transform(wmsCreateOrderDetailDTO,PclOrderB2cThirdDTO.class);
        pclOrderB2cThirdDTO.setReferenceNo(wmsCreateOrderDetailDTO.getOrderNo());
        //转换收件人信息
        pclOrderB2cThirdDTO.setRecipientName(wmsCreateOrderDetailDTO.getConsigneeName());
        pclOrderB2cThirdDTO.setRecipientCompany(wmsCreateOrderDetailDTO.getConsigneeCompanyName());
        pclOrderB2cThirdDTO.setPhone(wmsCreateOrderDetailDTO.getConsigneePhone());
        pclOrderB2cThirdDTO.setEmail(wmsCreateOrderDetailDTO.getConsigneeEmail());
        pclOrderB2cThirdDTO.setAddressLine1(wmsCreateOrderDetailDTO.getConsigneeAddress1());
        pclOrderB2cThirdDTO.setAddressLine2(wmsCreateOrderDetailDTO.getConsigneeAddress2());
        pclOrderB2cThirdDTO.setAddressLine3(wmsCreateOrderDetailDTO.getConsigneeAddress3());
        pclOrderB2cThirdDTO.setCountry(wmsCreateOrderDetailDTO.getConsigneeCountryCode());
        pclOrderB2cThirdDTO.setCity(wmsCreateOrderDetailDTO.getConsigneeCity());
        pclOrderB2cThirdDTO.setState(wmsCreateOrderDetailDTO.getConsigneeProvince());
        pclOrderB2cThirdDTO.setPostcode(wmsCreateOrderDetailDTO.getConsigneeZipCode());
        pclOrderB2cThirdDTO.setDistrict(wmsCreateOrderDetailDTO.getDistrict());
        //订单基础信息转换
        pclOrderB2cThirdDTO.setWeightUnit(WeightUnit.KG.getUnit());
        pclOrderB2cThirdDTO.setDimensionUnit(DimensionUnit.CM.getUnit());
        //转换商品信息
        List<B2cOrderProductDTO> b2cProductList = createWmsOrderDetailList.stream().map(b2cDTO -> {
            B2cOrderProductDTO b2cOrderProductDTO = new B2cOrderProductDTO();
            b2cOrderProductDTO.setSku(b2cDTO.getSku());
            b2cOrderProductDTO.setItemNo(b2cDTO.getSku());
            b2cOrderProductDTO.setDescription(b2cDTO.getProductNameEn());
            b2cOrderProductDTO.setNativeDescription(b2cDTO.getProductName());
            b2cOrderProductDTO.setUnitValue(b2cDTO.getExportValue());
            b2cOrderProductDTO.setUnitValueCurrency(b2cDTO.getExportValueCurrency());
            b2cOrderProductDTO.setItemCount(b2cDTO.getProductQty());
            b2cOrderProductDTO.setWeight(b2cDTO.getGrossWeightActual());
            b2cOrderProductDTO.setOriginCountry(b2cDTO.getShipperCountry());
            return b2cOrderProductDTO;
        }).collect(Collectors.toList());
        pclOrderB2cThirdDTO.setOrderItems(b2cProductList);
        B2cOrderProductDTO b2cOrderProductDTO = b2cProductList.get(0);
        BigDecimal totalValue = b2cProductList.stream().map(product -> {
            if (product.getUnitValue() == null) {
                return BigDecimal.ZERO;
            } else {
                Integer productCount = product.getItemCount();
                productCount = productCount != null ? productCount : 0;
                return product.getUnitValue().multiply(new BigDecimal(productCount.toString()));
            }
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal totalWeight = b2cProductList.stream().map(product -> {
            if (product.getWeight() == null) {
                return BigDecimal.ZERO;
            } else {
                Integer productCount = product.getItemCount();
                productCount = productCount != null ? productCount : 0;
                return product.getWeight().multiply(new BigDecimal(productCount.toString()));
            }
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
        pclOrderB2cThirdDTO.setWeight(totalWeight);
        pclOrderB2cThirdDTO.setInvoiceCurrency(b2cOrderProductDTO.getUnitValueCurrency());
        pclOrderB2cThirdDTO.setInvoiceValue(totalValue);
        String productDescription = b2cProductList.stream().map(B2cOrderProductDTO::getDescription).collect(Collectors.joining(","));
        if(Detect.notEmpty(productDescription)){
            if(productDescription.length()>200){
                pclOrderB2cThirdDTO.setDescription(productDescription.substring(0,200));
            }else{
                pclOrderB2cThirdDTO.setDescription(productDescription);
            }
        }
        String productNativeDescription = b2cProductList.stream().map(B2cOrderProductDTO::getNativeDescription).collect(Collectors.joining(","));
        if(Detect.notEmpty(productNativeDescription)){
            if(productNativeDescription.length()>200){
                pclOrderB2cThirdDTO.setNativeDescription(productNativeDescription.substring(0,200));
            }else{
                pclOrderB2cThirdDTO.setNativeDescription(productNativeDescription);
            }
        }
        return pclOrderB2cThirdDTO;
    }

    @Override
    public List<SubError> checkAssociatedOrder(Long id) {
        List<SubError>errorList=new ArrayList<>();
        List<PclOrderB2cDetailItemVO> productDetailList = pclBatchDao.findSkuBatchInfoByOrderId(id);
        //查询订单已关联的退货单 需要减去当前订单已经退货的商品数量
        List<PclProductReturn> returnedProducts = pclOrderReturnDao.findReturnProductsByOrderId(id);
        if(Detect.notEmpty(returnedProducts)){
            Map<String, Long> returnProductMap = returnedProducts.stream().collect(Collectors.groupingBy(PclProductReturn::getSku, Collectors.summingLong(PclProductReturn::getProductCount)));
            productDetailList.forEach(product ->{
                Long returnedCount = returnProductMap.get(product.getSku());
                if(returnedCount != null){
                    Long productTotalCount = product.getProductCount();
                    productTotalCount = productTotalCount != null ? productTotalCount : 0L;
                    product.setProductCount(productTotalCount - returnedCount);
                }
            });
        }
        //剔除商品数量小于等于0的商品
        productDetailList = productDetailList.stream().filter(p -> p.getProductCount() != null && p.getProductCount() > 0).collect(Collectors.toList());
        if(!Detect.notEmpty(productDetailList)){
            errorList.add(SubError.build(PclOrderReturnResultCode.ORDER_RETURN_ALL_PRODUCT_RETURN_NOT_ALLOW_ERROR));
        }
        return errorList;
    }

    @Override
    public List<PclOrderB2cExportDTO> findExportListWithSku(PclOrderB2cRequest request) {
        if(Objects.equals(request.getUseExport(),Boolean.TRUE) && CollectionUtils.isEmpty(request.getIdList())){
            CountHelper.startCount(CountBaseEntity.DEFAULT_COUNT_BASE);
            pclOrderB2cDao.selectByRequest(request);
        }
        return pclOrderB2cDao.findExportListWithSku(request);
    }

    @Override
    public List<PclOrderB2cExportDTO> findExportListWithoutSku(PclOrderB2cRequest request) {
        if(Objects.equals(request.getUseExport(),Boolean.TRUE) && CollectionUtils.isEmpty(request.getIdList())){
            CountHelper.startCount(CountBaseEntity.DEFAULT_COUNT_BASE);
            pclOrderB2cDao.selectByRequest(request);
        }
        return pclOrderB2cDao.findExportListWithoutSku(request);
    }

    private void handleMaerskAddress(WmsCreateOrderDTO wmsCreateOrderDTO) {
        String consigneeAddress1 = Objects.isNull(wmsCreateOrderDTO.getConsigneeAddress1()) ? "" : wmsCreateOrderDTO.getConsigneeAddress1();
        String consigneeAddress2 = Objects.isNull(wmsCreateOrderDTO.getConsigneeAddress2()) ? "" : wmsCreateOrderDTO.getConsigneeAddress2();
        String consigneeAddress3= wmsCreateOrderDTO.getConsigneeAddress3();
        if(StringUtils.isNotEmpty(consigneeAddress1) && consigneeAddress1.length()>35){
            String substringAddress1 = consigneeAddress1.substring(0, 35);
            //获取最后一个空格所在位置
            int address1LastWordIndex = substringAddress1.lastIndexOf(" ");
            if(address1LastWordIndex>0){
                String address1Result = substringAddress1.substring(0, address1LastWordIndex);
                wmsCreateOrderDTO.setConsigneeAddress1(address1Result);
            }else{
                wmsCreateOrderDTO.setConsigneeAddress1(substringAddress1);
            }
            //剩余地址放入地址2
            String substringAddress1ToAddress2 = consigneeAddress1.substring(address1LastWordIndex);
            consigneeAddress2=substringAddress1ToAddress2+consigneeAddress2;
            wmsCreateOrderDTO.setConsigneeAddress2(consigneeAddress2);
        }
        if(StringUtils.isNotEmpty(consigneeAddress2) && consigneeAddress2.length()>35){
            String substringAddress2 = consigneeAddress2.substring(0, 35);
            //获取最后一个空格所在位置
            int address2LastWordIndex = substringAddress2.lastIndexOf(" ");
            if(address2LastWordIndex>0){
                String address2Result = substringAddress2.substring(0, address2LastWordIndex);
                wmsCreateOrderDTO.setConsigneeAddress2(address2Result);
            }else{
                wmsCreateOrderDTO.setConsigneeAddress2(substringAddress2);
            }
            //剩余地址放入地址3
            String substringAddress1ToAddress3 = consigneeAddress2.substring(address2LastWordIndex);
            consigneeAddress3=substringAddress1ToAddress3+consigneeAddress3;
            wmsCreateOrderDTO.setConsigneeAddress3(consigneeAddress3);
        }
    }
    private List<SubError> checkOrderInventory(String orderNo,PartyVo shipper) {
        List<SubError> errorList=new ArrayList<>();
        List<PclOrderB2cDTO> pclOrderB2cDTOList = pclOrderB2cDao.findOrderAndProductByOrderNo(orderNo);
        pclOrderB2cDTOList.forEach(pclOrderB2cDTO -> {
            Integer skuCount = pclOrderB2cDTO.getItemCount();
            String sku = pclOrderB2cDTO.getSku();
            String warehouseCode = pclOrderB2cDTO.getShipperWarehouseCode();
            PclInventory pclInventory = pclInventoryDao.queryBySkuNoAndOverseasWarehouseCodeAndShipper(sku, warehouseCode, shipper.getTenantId(), shipper.getAggregator());
            if (pclInventory == null || null == pclInventory.getAvailableInventory()) {
                log.error("订单出库查询库存失败");
                errorList.add(SubError.build(PclOrderB2cResultCode.INVENTORY_NOT_FIND, pclOrderB2cDTO.getReferenceNo(), sku));
            } else if (pclInventory.getAvailableInventory() < skuCount) {
                log.error("订单：" + pclOrderB2cDTO.getReferenceNo() + ",商品:" + pclInventory.getSku() + ",在仓库" + pclInventory.getOverseasWarehouseName() + ",库存不足");
                errorList.add(SubError.build(PclOrderB2cResultCode.INSUFFICIENT_INVENTORY, pclOrderB2cDTO.getReferenceNo(), sku));
            }
        });
        return errorList;
    }

    @Override
    public List<SubError> batchResend(List<String> orderIds) {
        List<SubError> errorList = new ArrayList<>();
        //根据orderIds 获取订单信息
        List<PclOrderB2c> b2cOrderAndShipperDTOS = pclOrderB2cDao.selectByIds(orderIds);
        if (Detect.notEmpty(b2cOrderAndShipperDTOS)) {
            List<String> orderNoList = b2cOrderAndShipperDTOS.stream().map(PclOrderB2c::getOrderNo).filter(Objects::nonNull).collect(Collectors.toList());
            if (Detect.notEmpty(orderNoList)) {
                List<WmsCreateOrderDetailDTO> wmsCreateOrderDetailDTOList = pclOrderB2cDao.selectOrderAndProductInfoForWmsOrder(orderNoList);
                //wms仓库出库 获取出库订单需要的参数
                //添加wms 出库订单 更新订单信息 和更新库存信息
                createOderAndUpdateInventory(wmsCreateOrderDetailDTOList, errorList, null);
            }
        }
        return errorList;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    @OperationLog(handleDescription = "海外仓订单取消",serviceclass= PclOrderB2cServiceImpl.class,needDefaultCompare = true)
    public List<SubError> cancel(B2cOrderCancelRequest request) {
        List<SubError> errorList = new ArrayList<>();
        //效验状态是否为海外仓已接收
        PclOrderB2cDTO pclOrderB2cDTO = pclOrderB2cDao.selectOrderInfoById(request.getId());
        Short status = pclOrderB2cDTO.getStatus();
        //只有"已接收"和"出库异常"能取消订单
        if (!(B2cOrderStatus.CONFIRMED_OUTBOUND.getType().equals(status)
                || B2cOrderStatus.OUTBOUND_ABNORMALITY.getType().equals(status))) {
            errorList.add(SubError.build(PclOrderB2cResultCode.CAN_NOT_CANCEL));
            return errorList;
        }
        //调用wms 取消订单接口
        String orderNo = pclOrderB2cDTO.getOrderNo();
        Integer platform = pclOrderB2cDTO.getWarehousePlatform();
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();
        if(platform != null){
            WmsCancelOrderResultDTO wmsCancelOrderResultDTO = new WmsCancelOrderResultDTO();
            WmsHandleStrategyService handleStrategyService = wmsHandleServiceMap.get(platform);
            if (handleStrategyService != null) {
                wmsCancelOrderResultDTO = handleStrategyService.cancelOrder(pclOrderB2cDTO, request.getReason());
            } else {
                log.error("Method [{}],platform id [{}] can not match any handle service, please check...", "cancelOrder", platform);
            }
            PclOrderB2c pclOrderB2c = new PclOrderB2c();
            pclOrderB2c.setId(pclOrderB2cDTO.getId());
            pclOrderB2c.setCancelReason(request.getReason());
            pclOrderB2c.initUpdateEntity();
            //保存调用取消接口返回的message
            if(wmsCancelOrderResultDTO != null){
                String message = wmsCancelOrderResultDTO.getMessage();
                message = message != null && message.length() > 200 ? message.substring(0, 200) : message;
                pclOrderB2c.setCancelResult(message);
            }
            //调用取消接口后 根据返回结果改变当前订单状态
            if (wmsCancelOrderResultDTO != null && "Success".equals(wmsCancelOrderResultDTO.getAsk())) {
                //取消订单，过定时任务，不断获取，拦截成功则回复库存
                Boolean cancelSuccess = wmsCancelOrderResultDTO.getCancelSuccess();
                //初步判断取消订单返回结果(及时返回情况)
                if(cancelSuccess != null){
                    PclOrderB2c pclOrderB2c1 = pclOrderB2cDao.selectByPrimaryKey(pclOrderB2c.getId());
                    if(Objects.isNull(pclOrderB2c1)){
                        pclOrderB2c1=new PclOrderB2c();
                    }
                    if(cancelSuccess){
                        //拦截成功
                        pclOrderB2c.setDateCancel(new Date());
                        pclOrderB2c.setStatus(B2cOrderStatus.INTERCEPTION_SUCCEEDED.getType());
                        //拦截成功 撤销历史订单处理费
                        warehouseCalculateFeeService.reverseFee(OverseasWarehouseCostType.HANDLE, orderNo, "取消订单，冲减原始费用");
                        //创建事件
                        orderEventService.packageEvents(pclOrderB2c1.getOrderNo(),pclOrderB2c1.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.CANCELED);

                        recoveryInventory(pclOrderB2cDTO);
                    }else{
                        //拦截失败
                        pclOrderB2c.setStatus(B2cOrderStatus.CONFIRMED_OUTBOUND.getType());
                        orderEventService.packageEvents(pclOrderB2c1.getOrderNo(),pclOrderB2c1.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.ORDER_ACCEPTED);
                    }
                }else{
                    //拦截中 等待定时任务获取拦截结果
                    pclOrderB2c.setStatus(B2cOrderStatus.INTERCEPTING.getType());
                    //拦截中 不填充取消结果
                    pclOrderB2c.setCancelResult(null);
                    orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.INTERCEPTING);
                }
            } else {
                errorList.add(SubError.build(PclOrderB2cResultCode.CANCEL_ORDER_ERROR, wmsCancelOrderResultDTO.getMessage()));
            }
            orderEventService.addEventsByApi(events);
            pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
        }else{
            //非对接仓 直接取消
            PclOrderB2c pclOrderB2c = new PclOrderB2c();
            pclOrderB2c.setId(pclOrderB2cDTO.getId());
            pclOrderB2c.setCancelReason(request.getReason());
            pclOrderB2c.initUpdateEntity();
            //拦截成功
            pclOrderB2c.setDateCancel(new Date());
            pclOrderB2c.setStatus(B2cOrderStatus.INTERCEPTION_SUCCEEDED.getType());
            //拦截成功 撤销历史订单处理费
            warehouseCalculateFeeService.reverseFee(OverseasWarehouseCostType.HANDLE, orderNo, "取消订单，冲减原始费用");
            //创建事件
            orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.CANCELED);
            //恢复库存
            recoveryInventory(pclOrderB2cDTO);
            //添加事件
            orderEventService.addEventsByApi(events);
            //更新订单
            pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
        }
        return errorList;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public List<SubError> recoveryInventory(PclOrderB2cDTO pclOrderB2cDTO) {
        List<SubError>subErrors=new ArrayList<>();
        Long id = pclOrderB2cDTO.getId();
        Integer shipperId = pclOrderB2cDTO.getShipperId();
        String shipperWarehouseCode = pclOrderB2cDTO.getShipperWarehouseCode();
        List<PclProductB2c> productB2cList = pclProductB2cDao.findByOrderId(id);
        //按照sku进行分组
        productB2cList.stream().collect(Collectors.groupingBy(PclProductB2c::getSku)).forEach((sku, productList) -> {
            //获取该商品再对应仓库里的库存信息
            PclInventory skuInventory = pclInventoryDao.findBySkuAndWarehouseCode(sku, shipperWarehouseCode, shipperId);
            //1.恢复库存 (恢复出库订单的待出库库存到可用库存; 恢复出库订单的待出库库存)
            if (skuInventory != null) {
                //获取该订单出库的商品总数
                Optional<Long> productCountOptional = productList.stream().map(PclProductB2c::getProductCount).reduce(Long::sum);
                if (productCountOptional.isPresent()) {
                    Long totalProduct = productCountOptional.get();
                    pclInventoryDao.outgoingInventory(skuInventory, totalProduct.intValue()*-1, 0);
                }
            }else{
                subErrors.add(SubError.build(PclOrderB2cResultCode.ORDER_PRODUCT_INVENTORY_NOT_EXIST,sku));
            }
            //2.库存流水 状态变更为'复位'
            pclBatchDao.updateStatusByOrderId(pclOrderB2cDTO.getId(), BatchStatus.RESET.getStatus());
            //3.库龄 根据库存日志恢复库龄的剩余数量
            List<PclInventoryLog> pclInventoryLogList = pclInventoryLogDao.getByOrderId(pclOrderB2cDTO.getId());
            if(!Detect.notEmpty(pclInventoryLogList)){
                subErrors.add(SubError.build(PclOrderB2cResultCode.ORDER_INVENTORY_LOG_NOT_EXIST));
                log.info("订单:{} 库存日志不存在，无法恢复库龄", pclOrderB2cDTO.getOrderNo());
                return;
            }
            List<Long> inventoryAgingIds = pclInventoryLogList.stream().map(p -> p.getInventoryAgingId()).collect(Collectors.toList());
            List<PclInventoryAging> inventoryAgingList = pclInventoryAgingDao.findByIdList(inventoryAgingIds);
            Map<Long, List<PclInventoryLog>> inventoryLogMap = pclInventoryLogList.stream().collect(Collectors.groupingBy(p -> p.getInventoryAgingId()));
            //遍历库存日志记录的库龄 恢复剩余数量
            inventoryAgingList.forEach(inventoryAging ->{
                List<PclInventoryLog> inventoryLogList = inventoryLogMap.get(inventoryAging.getId());
                if(!Detect.notEmpty(inventoryLogList)){
                    return;
                }
                Integer changeCount = inventoryLogList.stream().map(p -> p.getChangeCount() != null ? p.getChangeCount() : 0).reduce(0, Integer::sum);
                Integer oldRemainingCount = inventoryAging.getRemainingCount();
                PclInventoryAging updateAging = new PclInventoryAging();
                updateAging.setId(inventoryAging.getId());
                updateAging.initUpdateEntity();
                updateAging.setRemainingCount(oldRemainingCount + changeCount);
                pclInventoryAgingDao.updateByPrimaryKeySelective(updateAging);
            });
        });
        return subErrors;
    }

    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRED)
    void createOderAndUpdateInventory(List<WmsCreateOrderDetailDTO> wmsCreateOrderDetailDTOList, List<SubError> errorList, List<String> unPostOrderNos) {
        //按海外仓代码对接平台分组,未对接得为null platform为0
        Map<Integer, List<WmsCreateOrderDetailDTO>> groupPlatformMap = wmsCreateOrderDetailDTOList.stream().collect(Collectors.groupingBy(x -> Optional.ofNullable(x.getWarehousePlatform()).orElse(0)));
        List<WmsCreateOrderResponseDTO> createOrderResponseList = new ArrayList<>();
        for (Integer platform : groupPlatformMap.keySet()) {
            List<WmsCreateOrderDetailDTO> wmsCreateOrderDTOLGroupPlatformList = groupPlatformMap.get(platform);
            if (platform.equals(0)) {
                wmsCreateOrderDTOLGroupPlatformList.stream().collect(Collectors.groupingBy(WmsCreateOrderDetailDTO::getOrderNo)).forEach((orderNo, groupByOrderNoList) -> {
                    WmsCreateOrderResponseDTO wmsCreateOrderResponseDTO = new WmsCreateOrderResponseDTO();
                    wmsCreateOrderResponseDTO.setAsk("Success");
                    wmsCreateOrderResponseDTO.setOrderNo(orderNo);
                    createOrderResponseList.add(wmsCreateOrderResponseDTO);
                });
            } else {
                WmsHandleStrategyService shippingPlanStrategyService = wmsHandleServiceMap.get(platform);
                if (shippingPlanStrategyService != null) {
                    createOrderResponseList.addAll(shippingPlanStrategyService.createOrderBatch(wmsCreateOrderDTOLGroupPlatformList));
                } else {
                    log.error("Method [{}],platform id [{}] can not match any handle service, please check...", "createOrder", platform);
                }
            }
        }
        updateInventoryAndCreateBatch(createOrderResponseList, errorList, unPostOrderNos);
    }

    /**
     * 生成待出库批次,更新待出库库存,计算费用
     *
     * @param createOrderResponseList
     * @param errorList
     * @param unPostOrderNos
     */
    void updateInventoryAndCreateBatch(List<WmsCreateOrderResponseDTO> createOrderResponseList, List<SubError> errorList, List<String> unPostOrderNos) {
        if (Detect.notEmpty(createOrderResponseList)) {
            List<String> orderNoList = new ArrayList<>();
            Set<String> collectedOrderNo = new HashSet<>();
            log.info("createOrderResponseList:{}", JSONObject.toJSONString(createOrderResponseList));
            List<TrkSourceOrderEventDTO> events=new ArrayList<>();
            createOrderResponseList.forEach(wmsCreateOrderResponseDTO -> {
                if ("Success".equals(wmsCreateOrderResponseDTO.getAsk())) {
                    List<SubError>orderError=new ArrayList<>();
                    List<PclOrderB2cDTO> pclOrderB2cDTOList = pclOrderB2cDao.findOrderAndProductByOrderNo(wmsCreateOrderResponseDTO.getOrderNo());
                    PclOrderB2cDTO pclOrderB2cDTO = pclOrderB2cDTOList.get(0);
                    Integer shipperId = pclOrderB2cDTO.getShipperId();
                    SysParty shipper = sysPartyService.getByPartyId(shipperId);
                    PclOrderB2c pclOrderB2c = new PclOrderB2c();
                    pclOrderB2c.setId(pclOrderB2cDTO.getId());
                    //设置提交时间
                    pclOrderB2c.setDateSending(new Date());
                    if (Detect.notEmpty(wmsCreateOrderResponseDTO.getOrderCode())) {
                        pclOrderB2c.setThirdOrderNo(wmsCreateOrderResponseDTO.getOrderCode());
                    } else if (unPostOrderNos != null) {
                        unPostOrderNos.add(wmsCreateOrderResponseDTO.getOrderNo());
                        collectedOrderNo.add(wmsCreateOrderResponseDTO.getOrderNo());
                    }
                    log.info("create b2cOrder batch:{}", JSONObject.toJSONString(pclOrderB2cDTOList));
                    //更新待出库库存
                    updateOutGoingInventory(pclOrderB2cDTOList, shipper, orderError);
                    if(!Detect.notEmpty(orderError)){
                        pclOrderB2c.initUpdateEntity();
                        pclOrderB2c.setStatus(B2cOrderStatus.CONFIRMED_OUTBOUND.getType());
                        //恢复异常原因为空
                        pclOrderB2c.setAbnormalCauses("");
                        pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
                        //生成待出库批次
                        createBatch(pclOrderB2cDTOList, shipper);
                        orderNoList.add(wmsCreateOrderResponseDTO.getOrderNo());
                        PclOrderB2c pclOrderB2c1 = pclOrderB2cDao.selectByPrimaryKey(pclOrderB2c.getId());
                        orderEventService.packageEvents(pclOrderB2c1.getOrderNo(),pclOrderB2c1.getReferenceNo(),pclOrderB2c1.getId(),events, WareHouseOrderEventCode.ORDER_ACCEPTED);
                    }else{
                        List<String> messageList = orderError.stream().map(SubError::getMessage).collect(Collectors.toList());
                        String messageStr= StringUtils.join(messageList, ";");
                        if(messageStr.length()>230){
                            String substringMessage = messageStr.substring(0, 230);
                            pclOrderB2c.setAbnormalCauses(substringMessage);
                        }else{
                            pclOrderB2c.setAbnormalCauses(messageStr);
                        }
                        pclOrderB2c.initUpdateEntity();
                        errorList.addAll(orderError);
                    }
                } else {
                    PclOrderB2c pclOrderB2c = pclOrderB2cDao.findByOrderNo(wmsCreateOrderResponseDTO.getOrderNo());
                    pclOrderB2c.initUpdateEntity();
                    pclOrderB2c.setAbnormalCauses(wmsCreateOrderResponseDTO.getErrMessage());
                    //海外仓下单失败 更新订单状态为「下单异常」
                    pclOrderB2c.setStatus(B2cOrderStatus.SENDING_ABNORMALITY.getType());
                    pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
                    errorList.add(SubError.build(PclOrderB2cResultCode.DESPATCH_OUTBOUND_ERROR,wmsCreateOrderResponseDTO.getOrderNo(), wmsCreateOrderResponseDTO.getErrMessage()));
                    orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.SENDING_ABNORMALITY);
                }
            });
            orderEventService.addEventsByApiNoRepetition(events);

            if (Detect.notEmpty(orderNoList)) {
                List<String> tmp = new ArrayList<>();
                for (String s : orderNoList) {
                    if (collectedOrderNo.contains(s)) {
                        continue;
                    }
                    tmp.add(s);
                }
                if (Detect.notEmpty(tmp)) {
                    // 计算 海外仓 处理费
                    calculatorHandleFee(tmp, true);
                }
            }
        }
    }

    private void updateOutGoingInventory(List<PclOrderB2cDTO> pclOrderB2cDTOList, SysParty shipper, List<SubError> errorList) {
        pclOrderB2cDTOList.forEach(pclOrderB2cDTO -> {
            Integer skuCount = pclOrderB2cDTO.getItemCount();
            Integer outCount = pclOrderB2cDTO.getItemCount();
            String sku = pclOrderB2cDTO.getSku();
            String warehouseCode = pclOrderB2cDTO.getShipperWarehouseCode();
            PclInventory pclInventory = pclInventoryDao.queryBySkuNoAndOverseasWarehouseCodeAndShipper(sku, warehouseCode, shipper.getTenantId(), shipper.getAggregator());
            if (pclInventory == null || null == pclInventory.getAvailableInventory()) {
                log.error("订单出库查询库存失败");
                errorList.add(SubError.build(PclOrderB2cResultCode.INVENTORY_NOT_FIND, pclOrderB2cDTO.getReferenceNo(), sku));
            } else if (pclInventory.getAvailableInventory() < skuCount) {
                log.error("订单：" + pclOrderB2cDTO.getReferenceNo() + ",商品:" + pclInventory.getSku() + ",在仓库" + pclInventory.getOverseasWarehouseName() + ",库存不足");
                errorList.add(SubError.build(PclOrderB2cResultCode.INSUFFICIENT_INVENTORY, pclOrderB2cDTO.getReferenceNo(), sku));
            } else {
                //根据不同的商品类型 扣减库龄剩余数量， 以及库存的可用数量/不可用数量，待出库数量
                if (Objects.equals(pclOrderB2cDTO.getInventoryType(), ProductInventoryType.DEFECTIVE_PRODUCT.getCode())) {
                    //不良品
                    List<PclInventoryAging> pclInventoryAgingList = pclInventoryAgingDao.findDefectiveByShipperAndSkuAndWarehouseCode(shipper.getTenantId(), warehouseCode, sku);
                    //按照库龄 优先库存时间久的。记录下需扣减的库龄数据，出库后扣除
                    for (PclInventoryAging pclInventoryAging : pclInventoryAgingList) {
                        pclInventoryAging.initUpdateEntity();
                        Integer remainingCount = pclInventoryAging.getDefectiveRemainingCount();
                        if (remainingCount == null) {
                            remainingCount = pclInventoryAging.getDefectiveCurrentCount();
                        }
                        if (remainingCount != 0) {
                            if (remainingCount.longValue() < skuCount) {
                                pclInventoryAging.setDefectiveRemainingCount(0);
                                skuCount = skuCount - remainingCount;
                                //生成库龄扣减记录，暂不更新库龄数据
                                createInventoryAgingLog(pclInventoryAging, remainingCount, pclOrderB2cDTO.getId(), ProductInventoryType.DEFECTIVE_PRODUCT.getCode());
                                pclInventoryAgingDao.updateByPrimaryKeySelective(pclInventoryAging);
                            } else if (remainingCount.longValue() == skuCount) {
                                pclInventoryAging.setDefectiveRemainingCount(0);
                                //生成库龄扣减记录，暂不更新库龄数据
                                createInventoryAgingLog(pclInventoryAging, skuCount, pclOrderB2cDTO.getId(), ProductInventoryType.DEFECTIVE_PRODUCT.getCode());
                                pclInventoryAgingDao.updateByPrimaryKeySelective(pclInventoryAging);
                                break;
                            } else {
                                pclInventoryAging.setDefectiveRemainingCount(remainingCount - skuCount);
                                //生成库龄扣减记录，暂不更新库龄数据
                                createInventoryAgingLog(pclInventoryAging, skuCount, pclOrderB2cDTO.getId(), ProductInventoryType.DEFECTIVE_PRODUCT.getCode());
                                pclInventoryAgingDao.updateByPrimaryKeySelective(pclInventoryAging);
                                break;
                            }
                        }
                    }
                    pclInventory.initUpdateEntity();
                    pclInventoryDao.outgoingInventory(pclInventory, 0, outCount);
                } else {
                    //良品
                    List<PclInventoryAging> pclInventoryAgingList = pclInventoryAgingDao.findByShipperAndSkuAndWarehouseCode(shipper.getTenantId(), warehouseCode, sku);
                    //按照库龄 优先库存时间久的。记录下需扣减的库龄数据，出库后扣除
                    for (PclInventoryAging pclInventoryAging : pclInventoryAgingList) {
                        pclInventoryAging.initUpdateEntity();
                        Integer remainingCount = pclInventoryAging.getRemainingCount();
                        if (remainingCount == null) {
                            remainingCount = pclInventoryAging.getCurrentCount();
                        }
                        if (remainingCount != 0) {
                            if (remainingCount.longValue() < skuCount) {
                                pclInventoryAging.setRemainingCount(0);
                                skuCount = skuCount - remainingCount;
                                //生成库龄扣减记录，暂不更新库龄数据
                                createInventoryAgingLog(pclInventoryAging, remainingCount, pclOrderB2cDTO.getId(), ProductInventoryType.GOOD_PRODUCT.getCode());
                                pclInventoryAgingDao.updateByPrimaryKeySelective(pclInventoryAging);
                            } else if (remainingCount.longValue() == skuCount) {
                                pclInventoryAging.setRemainingCount(0);
                                //生成库龄扣减记录，暂不更新库龄数据
                                createInventoryAgingLog(pclInventoryAging, skuCount, pclOrderB2cDTO.getId(), ProductInventoryType.GOOD_PRODUCT.getCode());
                                pclInventoryAgingDao.updateByPrimaryKeySelective(pclInventoryAging);
                                break;
                            } else {
                                pclInventoryAging.setRemainingCount(remainingCount - skuCount);
                                //生成库龄扣减记录，暂不更新库龄数据
                                createInventoryAgingLog(pclInventoryAging, skuCount, pclOrderB2cDTO.getId(), ProductInventoryType.GOOD_PRODUCT.getCode());
                                pclInventoryAgingDao.updateByPrimaryKeySelective(pclInventoryAging);
                                break;
                            }
                        }
                    }
                    pclInventory.initUpdateEntity();
                    pclInventoryDao.outgoingInventory(pclInventory, outCount, 0);
                }
            }
        });
    }

    private void createBatch(List<PclOrderB2cDTO> pclOrderB2cDTOList, SysParty shipper) {
        List<PclBatch> pclBatchList = new ArrayList<>();
        //根据sku分组 良品不良品数量合计
        pclOrderB2cDTOList.stream().collect(Collectors.groupingBy(p -> p.getSku())).forEach((sku, skuList) -> {
            PclOrderB2cDTO pclOrderB2cDTO = skuList.get(0);
            Integer skuCount = skuList.stream().map(p -> p.getItemCount()).filter(p -> p != null).reduce(0, Integer::sum);
            PclBatch pclBatch = new PclBatch();
            Date date = new Date();
            String batchNo = date.getTime() + pclOrderB2cDTO.getShipperWarehouseCode() + sku;
            SysProductDTO productDTO = sysProductDao.findByShipperAndSku(pclOrderB2cDTO.getShipperId(), sku, ProductType.OVERSEASWAREHOUSE.getType());
            SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(pclOrderB2cDTO.getShipperWarehouseCode(), pclOrderB2cDTO.getAggregator());
            pclBatch.setSku(sku);
            pclBatch.setReferenceNo(pclOrderB2cDTO.getReferenceNo());
            pclBatch.setOrderNo(pclOrderB2cDTO.getOrderNo());
            pclBatch.setSkuNameEn(productDTO.getSkuNameEn());
            pclBatch.setSkuNameLocal(productDTO.getSkuNameCn());
            pclBatch.setBatchNo(batchNo);
            pclBatch.setShipperId(pclOrderB2cDTO.getShipperId());
            pclBatch.setShipperName(shipper.getNameEn());
            pclBatch.setForecastAmount(skuCount);
            pclBatch.setOverseasWarehouseCode(pclOrderB2cDTO.getShipperWarehouseCode());
            pclBatch.setOverseasWarehouseName(sysOverseasWarehouse != null ? sysOverseasWarehouse.getName() : null);
            pclBatch.setOrderId(pclOrderB2cDTO.getId());
            pclBatch.setStatusUpdateTime(date);
            pclBatch.setStatus(BatchStatus.OUTBOUND_PENDING.getStatus());
            pclBatch.setInOrOutboundType(InOrOutboundType.OUTBOUND.getType());
            pclBatch.setSource(BatchSource.AUTOMATIC_BY_ORDER.getSource());
            pclBatchList.add(pclBatch);
        });
        if (Detect.notEmpty(pclBatchList)) {
            pclBatchDao.insertList(pclBatchList);
        }
    }


    /**
     * 海外仓b2c订单计算处理费,要根据订单编号寻找价卡，然后再进行计算
     * 触发时间，海外仓已接受之后
     *
     * @param orderNoList 订单编号列
     * @param isSave      是否保存账单数据 做测试用
     */
    @Override
    public List<SubError> calculatorHandleFee(List<String> orderNoList, boolean isSave) {
        long start = System.currentTimeMillis();
        List<SubError> result = Collections.emptyList();
        if(!Detect.notEmpty(orderNoList)){
            return result;
        }
        List<B2cOrderAndShipperDTO> b2cOrderList = pclOrderB2cDao.selectOrderAndShipperInfoByOrderNos(orderNoList);
        Map<String, SysOverseasWarehouseShipperRateDetailDTO> handleRateInfoMap = getOverseasWarehouseHandleRateConfig(b2cOrderList);
        Map<String, SysOverseasWarehouseShipperRateDetailDTO> lastMileBaseRateInfoMap = getOverseasWarehouseLastMileBaseRateConfig(b2cOrderList);
        Map<String, B2cOrderAndShipperDTO> b2cOrderNoMap = b2cOrderList.stream().collect(Collectors.toMap(p -> p.getOrderNo(), p -> p));
        Map<String, List<B2cProductInfo>> productInfoMap = getProductInfo(orderNoList);
        // 获取订单要转换的汇率
        Map<String, String> orderConvertCurrency = getConvertCurrency(orderNoList);
        // 中行汇率 map
        Map<String, SysChinaBankExchangeRate> rateMap = new HashMap<>();
        Date now = new Date();
        // 记录 订单处理费价卡配置的 币种
        Map<Long, String> currencyMap = new HashMap<>();
        for (String orderNo : orderNoList) {
            List<B2cProductInfo> productInfos = productInfoMap.get(orderNo);
            if (productInfos == null) {
                log.error("没有找到订单编号为 {} 的sku信息", orderNo);
                continue;
            }
            //获取订单对应的订单处理费价卡配置
            SysOverseasWarehouseShipperRateDetailDTO handleRateInfo = handleRateInfoMap.get(orderNo);
            if (handleRateInfo == null) {
                log.error("没有找到订单编号为 {} 的配置的海外仓价卡", orderNo);
                continue;
            }
            SysOverseasWarehouseShipperRateDetailDTO lastMileBaseRateInfo = lastMileBaseRateInfoMap.get(orderNo);
            Long handleId = handleRateInfo.getRateId();
            if (handleId == null) {
                log.error("{} 海外仓: {} 没有配置处理费价卡", orderNo, handleRateInfo.getWarehouseCode());
                continue;
            }
            // 获取价卡信息得到币种
            ActOverseasWarehouseRate rate = actStorageFeeRateDao.selectByPrimaryKey(handleId);
            if (rate == null) {
                log.error("配置的出库处理费价卡id：{}不存在请检查！", handleId);
                continue;
            }
            if (!Objects.equals(rate.getActive(), ActiveTypeNew.ACTIVE.getType())) {
                log.info("配置的出库处理费价卡id:{} 被禁用", handleId);
                continue;
            }
            String handleCurrency = Detect.notEmpty(rate.getCurrency()) ? rate.getCurrency() : "CNY";
            currencyMap.put(handleId, handleCurrency);

            ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, now);
            BigDecimal totalAmount = BigDecimal.ZERO;
            //计费明细
            List<ActOverseasWarehouseBillingItemHandling> billingItemList = Lists.newArrayList();
            //未配置价卡则费用为0
            if (Detect.notEmpty(rateDetail.getActHandleReturnDetails())) {
                if (rate != null && rate.getBillingType() != null && rate.getBillingType().equals(OverseasWarehouseRateBillingType.ORDER.getType())) {
                    totalAmount = calculateOrderFee(productInfos, rateDetail, rate, handleRateInfo, billingItemList);
                } else {
                    for (B2cProductInfo productInfo : productInfos) {
                        BigDecimal amount = calculateProductFee(productInfo, rateDetail.getActHandleReturnDetails(), rate, lastMileBaseRateInfo, billingItemList);
                        totalAmount = totalAmount.add(amount);
                    }
                }
            }
            // 计算到价卡
            ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
            create.setOrderNo(orderNo);
            // 此变量用来判断 打完折 总金额 如果是0 的话 就不计算
            boolean discountAfter = false;
            //计算打折
            if (handleRateInfo != null && handleRateInfo.getDiscount() != null) {
                Integer handleCostDiscount = handleRateInfo.getDiscount();
                if (handleCostDiscount != 0) {
                    create.setHandleCostDiscount(handleCostDiscount);
                    double multiplyNumber = 1 - handleCostDiscount * 0.01;
                    totalAmount = totalAmount.multiply(new BigDecimal(Double.toString(multiplyNumber)));
                    if (BigDecimalUtils.eq(BigDecimalUtils.ZERO, totalAmount)) {
                        discountAfter = true;
                    }
                }
            }
            if (discountAfter) {
                continue;
            }
            //>>>>>记录商品计费明细 start>>>>>
            for (ActOverseasWarehouseBillingItemHandling billingItem : billingItemList) {
                Integer discount = handleRateInfo.getDiscount();
                BigDecimal rateCardAmount = BigDecimalUtils.covertNullToZero(billingItem.getRateCardAmount());
                billingItem.setHitRateId(handleId);
                billingItem.setDiscount(discount);
                billingItem.setRateCardCurrency(handleCurrency);
                billingItem.setDiscountedRateCardAmount(rateCardAmount);
                if (discount != null && discount != 0) {
                    BigDecimal discountedRateCardAmount = rateCardAmount.multiply(new BigDecimal(Double.toString(1 - discount * 0.01)));
                    billingItem.setDiscountedRateCardAmount(discountedRateCardAmount);
                }
            }
            //>>>>>记录商品计费明细 end>>>>>
            create.setHandlingBillingItems(billingItemList);
            create.setAmount(totalAmount);
            //处理HST/GST/PST消费税
            actRateHelper.handleSalesTaxRateFee(handleRateInfo != null ? handleRateInfo.getSalesTaxRateId() : null, now,  create);
            create.setCostType(OverseasWarehouseCostType.HANDLE.getType());
            create.setOverseasWarehouseCode(handleRateInfo.getWarehouseCode());
            create.setShipperId(handleRateInfo.getShipper());
            create.setShipperName(handleRateInfo.getShipperName());
            create.setCurrency(handleCurrency);
            // 变换币种
            String curCurrency = create.getCurrency();
            BigDecimal curAmount = create.getAmount();
            String newCurrency = orderConvertCurrency.get(orderNo);
            final BigDecimal convertAmount = sysChinaBankExchangeRateService.getProductInvoiceValue(curAmount,
                    curCurrency, newCurrency, rateMap);
            create.setConvertAmount(convertAmount);
            create.setConvertCurrency(newCurrency);
            create.setSource(BillSource.AUTOMATIC.getType());
            B2cOrderAndShipperDTO b2cOrderAndShipperDTO = b2cOrderNoMap.get(orderNo);
            if (b2cOrderAndShipperDTO != null) {
                create.setReferenceNo(b2cOrderAndShipperDTO.getReferenceNo());
                create.setTrackingNo(b2cOrderAndShipperDTO.getTrackingNo());
            }
            List<SubError> subErrors = actOverseasWarehouseBillService.consumeBill(create);
            if (Detect.notEmpty(subErrors)) {
                log.error("error happens while cal handle fee: {}", subErrors);
                result = subErrors;
            }
        }
        log.info("计算处理费用时:{}", System.currentTimeMillis() - start);
        return result;
    }

    private BigDecimal calculateOrderFee(List<B2cProductInfo> productInfos, ActHandleReturnDetailDTO actHandleReturnDetailDTO, ActOverseasWarehouseRate rate, SysOverseasWarehouseShipperRateDetailDTO rateInfo, List<ActOverseasWarehouseBillingItemHandling> billingItemList) {
        B2cProductInfo b2cProductInfo = productInfos.get(0);
        BigDecimal totalAmount;
        BigDecimal additionalItemFee = actHandleReturnDetailDTO.getAdditionalItemFee();
        BigDecimal additionalWeightFee = actHandleReturnDetailDTO.getAdditionalWeightFee();
        List<ActHandleReturnDetail> actHandleReturnDetails = actHandleReturnDetailDTO.getActHandleReturnDetails();
        //订单总商品数量
        long productCount = productInfos.stream().filter(productInfo -> productInfo.getProductCount() != null).collect(Collectors.summarizingLong(B2cProductInfo::getProductCount)).getSum();
        if (productCount == 0) {
            log.error("订单商品总数量为0: {}", b2cProductInfo.getOrderNo());
            return BigDecimal.ZERO;
        }
        BigDecimal weight=productInfos.stream().map(productInfo -> {
            BigDecimal skuVolume=BigDecimalUtils.covertNullToZero(productInfo.getProductHeightActual()
                    .multiply(BigDecimalUtils.covertNullToZero(productInfo.getProductWidthActual()))
                    .multiply(BigDecimalUtils.covertNullToZero(productInfo.getProductLengthActual())));
            BigDecimal skuWeight = productInfo.getWeight() == null ? BigDecimal.ZERO : productInfo.getWeight();
            BigDecimal productBillingWeight = calculateOrderBubbleWeight(b2cProductInfo.getOrderNo(),skuWeight,skuVolume,rate,rateInfo).multiply(new BigDecimal(productInfo.getProductCount()));
            //>>>>>记录商品计费明细 start>>>>>
            ActOverseasWarehouseBillingItemHandling billingItemHandling = recordHandlingBillingItemInfo(productInfo);
            billingItemHandling.setMatchingWeight(productBillingWeight);
            billingItemList.add(billingItemHandling);
            //>>>>>记录商品计费明细 end>>>>>
            return productBillingWeight;
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
        //总商品重量
        ActHandleReturnDetail selectedDetail = null;
        for (ActHandleReturnDetail rateDetail : actHandleReturnDetails) {
            BigDecimal weightStart = rateDetail.getWeightStart();
            BigDecimal weightEnd = rateDetail.getWeightEnd();
            if (weightEnd != null && BigDecimalUtils.gt(weight, weightEnd)) {
                continue;
            }
            if (weightStart != null && BigDecimalUtils.le(weight, weightStart)) {
                continue;
            }
            selectedDetail = rateDetail;
            break;
        }
        if (selectedDetail == null) {
            ActHandleReturnDetail minActHandleReturnDetail = actHandleReturnDetails.stream().min(Comparator.comparing(ActHandleReturnDetail::getWeightStart)).get();
            ActHandleReturnDetail maxActHandleReturnDetail = actHandleReturnDetails.stream().max(Comparator.comparing(ActHandleReturnDetail::getWeightEnd)).get();
            if (!BigDecimalUtils.lt(weight, minActHandleReturnDetail.getWeightStart())&&BigDecimalUtils.gt(weight, maxActHandleReturnDetail.getWeightEnd())) {
                selectedDetail = maxActHandleReturnDetail;
            }
        }
        BigDecimal weightEnd = BigDecimal.ZERO;
        BigDecimal itemPrice = BigDecimal.ZERO;
        if (selectedDetail != null) {
            weightEnd = selectedDetail.getWeightEnd();
            itemPrice = selectedDetail.getItemPrice();
            if (itemPrice == null) {
                log.error("没有设置计费单价价卡OrderNo: {}", b2cProductInfo.getOrderNo());
                return BigDecimal.ZERO;
            }
        }
        BigDecimal totalAdditionalWeightFee = BigDecimal.ZERO;
        BigDecimal totalAdditionalItemFee = BigDecimal.ZERO;
        if (weightEnd.compareTo(BigDecimal.ZERO)!=0 && BigDecimalUtils.gt(weight, weightEnd)) {
            BigDecimal additionalWeight = weight.subtract(weightEnd);
            totalAdditionalWeightFee = additionalWeight.multiply(additionalWeightFee);
        }
        if (productCount > 1) {
            long additionalItem = productCount - 1;
            totalAdditionalItemFee = BigDecimalUtils.covertNullToZero(additionalItemFee).multiply(new BigDecimal(String.valueOf(additionalItem)));
        }
        totalAmount = itemPrice.add(totalAdditionalWeightFee).add(totalAdditionalItemFee);
        //>>>>>记录商品计费明细 start>>>>>
        for (ActOverseasWarehouseBillingItemHandling billingItem : billingItemList) {
            billingItem.setBillingType(OverseasWarehouseRateBillingType.ORDER.getType());
            billingItem.setHitPrice(itemPrice);
            billingItem.setAdditionalWeightPrice(totalAdditionalWeightFee);
            billingItem.setAdditionalItemPrice(totalAdditionalItemFee);
            billingItem.setHitRateId(rateInfo.getRateId());
            billingItem.setDiscount(rateInfo.getDiscount());
            billingItem.setRateCardAmount(totalAmount);
        }
        //>>>>>记录商品计费明细 end>>>>>
        return totalAmount;
    }

    /**
     * 记录出库处理费商品明细
     * @param productInfo
     * @return
     */
    private ActOverseasWarehouseBillingItemHandling recordHandlingBillingItemInfo(B2cProductInfo productInfo) {
        ActOverseasWarehouseBillingItemHandling billingItemHandling = new ActOverseasWarehouseBillingItemHandling();
        billingItemHandling.setSku(Detect.notEmpty(productInfo.getSku()) ? productInfo.getSku() : "");
        billingItemHandling.setThirdSkuNo(productInfo.getThirdSkuNo());
        billingItemHandling.setProductWeight(productInfo.getWeight());
        billingItemHandling.setProductLength(productInfo.getProductLengthActual());
        billingItemHandling.setProductWidth(productInfo.getProductWidthActual());
        billingItemHandling.setProductHeight(productInfo.getProductHeightActual());
        billingItemHandling.setWeightUnit(WeightUnit.KG.getUnit().toUpperCase());
        billingItemHandling.setDimensionUnit(DimensionUnit.CM.getUnit().toUpperCase());
        billingItemHandling.setProductQty(productInfo.getProductCount() != null ? productInfo.getProductCount().intValue() : null);
//        billingItemHandling.setBillingType();
//        billingItemHandling.setMatchingWeight();
//        billingItemHandling.setHitRateId();
//        billingItemHandling.setHitPrice();
//        billingItemHandling.setAdditionalWeightPrice();
//        billingItemHandling.setAdditionalItemPrice();
//        billingItemHandling.setDiscount();
//        billingItemHandling.setRateCardAmount();
//        billingItemHandling.setDiscountedRateCardAmount();
//        billingItemHandling.setRateCardCurrency();
        return billingItemHandling;
    }


    private Map<String, String> getConvertCurrency(List<String> orderNoList) {
        if (orderNoList == null || orderNoList.size() == 0) {
            return Collections.emptyMap();
        }
        List<PclOrderConvertVO> list = pclOrderB2cDao.getConvertCurrencyByOrderNo(orderNoList);
        if (list == null || list.size() == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> map = new HashMap<>();
        for (PclOrderConvertVO pclOrderConvertVO : list) {
            map.put(pclOrderConvertVO.getOrderNo(), pclOrderConvertVO.getCurrency());
        }
        return map;
    }

    @Override
    public List<B2cOrderForWmsOrder> findNeedGetOrderStatusOrders(Integer aggregator, Integer platformCode) {
        return pclOrderB2cDao.findNeedGetOrderStatusOrders(aggregator, platformCode);
    }

    @Override
    public List<B2cOrderForWmsOrder> findNeedGetCancelStatusOrders(Integer aggregator, Integer platformCode) {
        return pclOrderB2cDao.findNeedGetCancelStatusOrders(aggregator, platformCode);
    }

    private Map<String, ActOverseasWarehouseShipperDiscountInfo> getOverseasWarehouseShipperDiscountByOrderNo(List<String> orderNoList) {
        List<ActOverseasWarehouseShipperDiscountInfo> discountInfoList = pclOrderB2cDao.findWarehouseShipperDiscountByOrderNoList(orderNoList);
        return discountInfoList.stream().collect(Collectors.toMap(ActOverseasWarehouseShipperDiscountInfo::getOrderNo, ActOverseasWarehouseShipperDiscountInfo -> ActOverseasWarehouseShipperDiscountInfo));
    }

    @Override
    public List<B2cOrderForQueryCostDTO> findNeedGetCostOrder(Integer pastDays) {
        return  pclOrderB2cDao.findNeedGetCostOrder(pastDays);
    }

    @Override
    public List<B2cOrderForQueryCostDTO> findNeedCalculateLastMileFee() {
        return  pclOrderB2cDao.findNeedCalculateLastMileFee();
    }

    @Override
    public void updateActualFeeByTrackingNo(String trackingNo, BigDecimal actualShippingFee, String currency) {
        pclOrderB2cDao.updateActualFeeByTrackingNo(trackingNo, actualShippingFee, currency);
    }

    @Override
    public PclOrderB2c getByTrackingNo(String trackingNo) {
        return pclOrderB2cDao.selectByTrackingNo(trackingNo);
    }

    @Override
    public PclOrderB2cDetailVO detail(Long id) {
        PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(id);
        PclOrderB2cDetailVO pclOrderB2cDetailVO = BeanUtils.transform(pclOrderB2c, PclOrderB2cDetailVO.class);
        //发货人地址
        String shipperAddressId = pclOrderB2c.getShipperAddressId();
        if (Detect.notEmpty(shipperAddressId)) {
            SysAddress shipperAddress = addressService.get(shipperAddressId);
            pclOrderB2cDetailVO.setShipperAddress(shipperAddress);
        }
        //收货人地址
        String recipientAddressId = pclOrderB2c.getRecipientAddressId();
        if (Detect.notEmpty(recipientAddressId)) {
            SysAddress consigneeAddress = addressService.get(recipientAddressId);
            pclOrderB2cDetailVO.setRecipientAddress(consigneeAddress);
        }
        //是否启用地址簿 recipientWarehouseCode为空表示未启用地址簿
        String recipientWarehouseCode = pclOrderB2c.getRecipientWarehouseCode();
        if(!Detect.notEmpty(recipientWarehouseCode)){
            pclOrderB2cDetailVO.setUseWarehouseCode(false);
        }else{
            pclOrderB2cDetailVO.setUseWarehouseCode(true);
        }
        //订单可编辑状态（已创建，下单异常、出库异常），查询详情接口置空channelId
        if (B2cOrderStatus.getEditableStatus().contains(B2cOrderStatus.get(pclOrderB2c.getStatus()))) {
            List<SysOverseasWarehouseChannelSimpleDTO> activeWarehouseChannels = sysOverseasWarehouseChannelDao.getByWarehouseCodeAndShipperChannelActive(
                    pclOrderB2c.getShipperId(), pclOrderB2c.getShipperWarehouseCode(), ChannelType.FULFILMENT.getType(), ActiveTypeNew.ACTIVE.getType());
            SysOverseasWarehouseChannelSimpleDTO targetWarehouseChannel = activeWarehouseChannels.stream().filter(p -> Objects.equals(p.getId(), pclOrderB2c.getChannelId())).findFirst().orElse(null);
            if ( targetWarehouseChannel == null) {
                pclOrderB2cDetailVO.setChannelId(null);
            } else {
                pclOrderB2cDetailVO.setIsOffline(targetWarehouseChannel.getIsOffline());
            }
        }
        String shipperWarehouseCode = pclOrderB2c.getShipperWarehouseCode();
        SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(shipperWarehouseCode, pclOrderB2c.getAggregator());
        //获取订单的商品信息
        List<PclOrderB2cDetailItemVO> productDetailList = pclProductB2cDao.findDetailInfoByOrderId(pclOrderB2c.getId());
        productDetailList.forEach(pclOrderB2cDetailItemVO -> {
            Short shippingBagPackedCode = pclOrderB2cDetailItemVO.getShippingBagPacked();
            ShippingBagPacked shippingBagPacked = ShippingBagPacked.get(shippingBagPackedCode);
            if (shippingBagPacked != null) {
                pclOrderB2cDetailItemVO.setShippingBagPackedMsg(shippingBagPacked.getMessage());
            }
            ProductInventoryType inventoryType = ProductInventoryType.get(pclOrderB2cDetailItemVO.getInventoryType());
            pclOrderB2cDetailItemVO.setInventoryTypeMsg(inventoryType != null ? inventoryType.getMessage() : null);
            //马士基对接需要转换单位
            if(sysOverseasWarehouse.getPlatForm()!=null&&sysOverseasWarehouse.getPlatForm().equals(OverseasWarehousePlatform.MAERSK.getPlatformCode())){
                convertProductUnit(pclOrderB2cDetailItemVO);
            }
            //单位处理 实际有值使用实际的默认单位CM,KG，否则使用预估单位 前端不方便计算处理（暂时表中没有存储实际单位）
            //1.重量
            if(!Objects.isNull(pclOrderB2cDetailItemVO.getGrossWeightActual())){
                pclOrderB2cDetailItemVO.setWeightUnit(WeightUnit.KG.getUnit().toUpperCase());
            }
            //2.尺寸
            BigDecimal productLengthActual = pclOrderB2cDetailItemVO.getProductLengthActual();
            BigDecimal productWidthActual = pclOrderB2cDetailItemVO.getProductWidthActual();
            BigDecimal productHeightActual = pclOrderB2cDetailItemVO.getProductHeightActual();
            if(!Objects.isNull(productLengthActual) || !Objects.isNull(productWidthActual)
                    || !Objects.isNull(productHeightActual)){
                pclOrderB2cDetailItemVO.setDimensionUnit(DimensionUnit.CM.getUnit().toUpperCase());
            }
        });
        pclOrderB2cDetailVO.setPclOrderB2cDetailItemList(productDetailList);
        //转换体积单位 已经在定时中转成CBM
        //订单关联费用账单
        List<ActOverseasWarehouseBillMergeDTO> billMergeDTOs = actOverseasWarehouseBillService.getBillItemByOrderNoOrTrackingNo(pclOrderB2c.getOrderNo(), pclOrderB2c.getTrackingNo());
        pclOrderB2cDetailVO.setBillItems(billMergeDTOs);
        pclOrderB2cDetailVO.setWarehousePlatform(sysOverseasWarehouse.getPlatForm());
        //订单下商品种类数和商品件数
        long productCategoryCount = productDetailList.stream().map(p -> p.getSku()).distinct().count();
        long productPieceCount = productDetailList.stream().map(p -> p.getProductCount()).reduce(0L, Long::sum);
        pclOrderB2cDetailVO.setProductCategoryCount(productCategoryCount);
        pclOrderB2cDetailVO.setProductCategoryCount(productPieceCount);
        return pclOrderB2cDetailVO;
    }

    private void convertProductUnit(PclOrderB2cDetailItemVO pclOrderB2cDetailItemVO) {
        String dimensionUnit = pclOrderB2cDetailItemVO.getDimensionUnit();
        String weightUnit = pclOrderB2cDetailItemVO.getWeightUnit();
        if(WeightUnit.LB.equals(WeightUnit.getByCode(weightUnit))){
            BigDecimal convertedWeight = UnitUtil.convertUnit(pclOrderB2cDetailItemVO.getGrossWeight(), WeightUnit.LB, WeightUnit.KG);
            pclOrderB2cDetailItemVO.setGrossWeightActual(convertedWeight);
        }else{
            pclOrderB2cDetailItemVO.setGrossWeightActual(pclOrderB2cDetailItemVO.getGrossWeight());
        }
        if(DimensionUnit.IN.equals(DimensionUnit.getByCode(dimensionUnit))){
            BigDecimal convertedLength = UnitUtil.convertUnit(pclOrderB2cDetailItemVO.getProductLength(), DimensionUnit.IN, DimensionUnit.CM);
            BigDecimal convertedHeight = UnitUtil.convertUnit(pclOrderB2cDetailItemVO.getProductHeight(), DimensionUnit.IN, DimensionUnit.CM);
            BigDecimal convertedWidth = UnitUtil.convertUnit(pclOrderB2cDetailItemVO.getProductWidth(), DimensionUnit.IN, DimensionUnit.CM);
            pclOrderB2cDetailItemVO.setProductLengthActual(convertedLength.setScale(3, RoundingMode.HALF_UP));
            pclOrderB2cDetailItemVO.setProductHeightActual(convertedHeight.setScale(3, RoundingMode.HALF_UP));
            pclOrderB2cDetailItemVO.setProductWidthActual(convertedWidth.setScale(3, RoundingMode.HALF_UP));
        }else{
            pclOrderB2cDetailItemVO.setProductLengthActual(pclOrderB2cDetailItemVO.getProductLength().setScale(3, RoundingMode.HALF_UP));
            pclOrderB2cDetailItemVO.setProductHeightActual(pclOrderB2cDetailItemVO.getProductHeight().setScale(3, RoundingMode.HALF_UP));
            pclOrderB2cDetailItemVO.setProductWidthActual(pclOrderB2cDetailItemVO.getProductWidth().setScale(3, RoundingMode.HALF_UP));
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public List<SubError> addOrEdit(PclOrderB2cDetailVO pclOrderB2cDetailVO) {
        log.info("处理字符串字段前后空格");
        FieldTrimProcessor.trimObject(pclOrderB2cDetailVO);
        List<SubError> errorList = new ArrayList<>();
        Long id = pclOrderB2cDetailVO.getId();
        //是否启用地址簿 recipientWarehouseCode为空表示未启用地址簿
        Boolean useWarehouseCode = pclOrderB2cDetailVO.getUseWarehouseCode();
        if(useWarehouseCode == null || !useWarehouseCode){
            pclOrderB2cDetailVO.setRecipientWarehouseCode("");
        }
        //判断是否需要切换服务
        SysOverseasWarehouseChannelDTO sysOverseasWarehouseChannelDTO= sysOverseasWarehouseChannelDao.selectChannelDetailByOverseasChannelId(pclOrderB2cDetailVO.getChannelId());
        //判断服务是否已被删除
        if (sysOverseasWarehouseChannelDTO == null) {
           errorList.add(SubError.build(ChannelResultCode.CHANNEL_NOT_EXIST));
        }
        //校验附件
        checkAttachmentFiles(pclOrderB2cDetailVO, errorList);
        if (Detect.notEmpty(errorList)) {
            return errorList;
        }
        PclOrderB2cSwitchChannelDTO pclOrderB2cSwitchChannelDTO=BeanUtils.transform(pclOrderB2cDetailVO,PclOrderB2cSwitchChannelDTO.class);
        //完善地址信息
        BeanUtils.transform(pclOrderB2cDetailVO.getRecipientAddress(),pclOrderB2cSwitchChannelDTO);
        pclOrderB2cSwitchChannelDTO.setVirtualChannelName(sysOverseasWarehouseChannelDTO.getVirtualChannelName());
        PclOrderB2cSwitchChannelResultDTO switchChannelDTO = getSwitchChannelDTO(pclOrderB2cSwitchChannelDTO);
        BeanUtils.transform(switchChannelDTO,pclOrderB2cDetailVO);
        //渠道规则校验
        List<SysAddressRuleConfig> addressRuleConfigList = sysAddressRuleConfigDao.getActiveByChannelId(pclOrderB2cDetailVO.getChannelId());
        List<PclOrderB2cDetailItemVO> detailItemList = pclOrderB2cDetailVO.getPclOrderB2cDetailItemList();
        if(CollectionUtils.isNotEmpty(addressRuleConfigList) && CollectionUtils.isNotEmpty(detailItemList)){
            List<SubError> addressErrors = sysAddressRuleConfigService.validateSavePclOrderB2cInfo(pclOrderB2cDetailVO, addressRuleConfigList);
            if(CollectionUtils.isNotEmpty(addressErrors)){
                errorList.addAll(addressErrors);
            }
        }
        //编辑
        if (id != null) {
            editB2cOrderByDetailVO(pclOrderB2cDetailVO, errorList);
        }
        //添加
        else {
            //验重
            List<PclOrderB2c> repeatList = pclOrderB2cDao.findByReferenceNo(pclOrderB2cDetailVO.getReferenceNo(), pclOrderB2cDetailVO.getShipperId());
            if (Detect.notEmpty(repeatList)) {
                errorList.add(SubError.build(PclOrderResultCode.ORDER_REF_NO_UNIQUE, pclOrderB2cDetailVO.getReferenceNo()));
                return errorList;
            }
            //转化导入的信息
            PclOrderB2c pclOrderB2c = createB2cOrderByDetailVO(pclOrderB2cDetailVO,errorList);
            //创建事件
            List<TrkSourceOrderEventDTO> events=new ArrayList<>();
            orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.ORDER_CREATED);
            orderEventService.addEventsByApi(events);

            //出入b2c订单关联的商品
            createB2cOrderProduct(pclOrderB2c, pclOrderB2cDetailVO);
        }
        return errorList;
    }

    private void checkAttachmentFiles(PclOrderB2cDetailVO pclOrderB2cDetailVO, List<SubError> errorList) {
        List<PclOrderB2cAttachment> fileList = pclOrderB2cDetailVO.getFileList();
        if (Detect.empty(fileList)) {
            return;
        }
        List<PclOrderB2cAttachment> otherFileList = fileList.stream().filter(p -> Objects.equals(p.getFileType(), OrderAttachmentType.FILE.getType())).collect(Collectors.toList());
        List<PclOrderB2cAttachment> labelFileList = fileList.stream().filter(p -> Objects.equals(p.getFileType(), OrderAttachmentType.LABEL.getType())).collect(Collectors.toList());

        String shipperWarehouseCode = pclOrderB2cDetailVO.getShipperWarehouseCode();
        SysOverseasWarehouse warehouse = sysOverseasWarehouseDao.queryByCode(shipperWarehouseCode, SessionContext.getContext().getAggregator());
        //易仓WMS 上传标签附件校验
        if (warehouse != null && OverseasWarehousePlatform.getplatformCodeListByPlatform("ECCANG").contains(warehouse.getPlatForm())) {
            //标签文件
            if (Detect.notEmpty(labelFileList)) {
                List<String> allowLabelExtensions = Arrays.asList("pdf");
                List<String> labelSuffixTypes = labelFileList.stream().map(p -> p.getFileName().substring(p.getFileName().lastIndexOf(".") + 1)).distinct().collect(Collectors.toList());
                if (!allowLabelExtensions.containsAll(labelSuffixTypes)) {
                    errorList.add(SubError.build(WmsEcResultCode.UPLOAD_LABEL_FILE_TYPE_ERROR));
                }
            }
            //其他文件
            if (Detect.notEmpty(otherFileList)) {
                List<String> allowedTypeList= Lists.newArrayList("pdf","rar","zip");
                List<String> suffixTypes = otherFileList.stream().map(p -> p.getFileName().substring(p.getFileName().lastIndexOf(".") + 1)).distinct().collect(Collectors.toList());
                if (!allowedTypeList.containsAll(suffixTypes)) {
                    errorList.add(SubError.build(WmsEcResultCode.UPLOAD_FILE_TYPE_ERROR));
                }
            }
        }

    }

    /**
     * 调用第三方接口获取地址类型，判断是否需要切换服务
     * 返回切换后结果
     * @param pclOrderB2cSwitchChannelDTO
     */
    @Override
    public PclOrderB2cSwitchChannelResultDTO getSwitchChannelDTO(PclOrderB2cSwitchChannelDTO pclOrderB2cSwitchChannelDTO) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        PclOrderB2cSwitchChannelResultDTO pclOrderB2cSwitchChannelResultDTO=new PclOrderB2cSwitchChannelResultDTO();
        pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
        SysOverseasWarehouseChannelDTO oldSysOverseasWarehouseChannelDTO = sysOverseasWarehouseChannelDao.selectChannelDetailByOverseasChannelName(pclOrderB2cSwitchChannelDTO.getShipperWarehouseCode(), pclOrderB2cSwitchChannelDTO.getVirtualChannelName(), aggregator, ChannelType.FULFILMENT.getType());
        if(oldSysOverseasWarehouseChannelDTO!=null&&oldSysOverseasWarehouseChannelDTO.getAddressValidationTypeId()!=null){
            List<SysAddressValidationTypeShipper> sysAddressValidationTypeShipperList = sysAddressValidationTypeShipperDao.selectByShipperId(pclOrderB2cSwitchChannelDTO.getShipperId());
            if(!Detect.notEmpty(sysAddressValidationTypeShipperList)){
                pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
            }else{
                sysAddressValidationTypeShipperList.forEach(sysAddressValidationTypeShipper -> {
                    Long shipperAddressValidationTypeId = sysAddressValidationTypeShipper.getAddressValidationTypeId();
                    if(shipperAddressValidationTypeId.equals(oldSysOverseasWarehouseChannelDTO.getAddressValidationTypeId())){
                        //匹配则需要进行地址校验
                        ValidationAddressDTO validationAddressDTO=getValidationAddressDTO(pclOrderB2cSwitchChannelDTO);
                        FedexValidationAddressResponse fedexValidationAddressResponse = fedexService.validationAddress(validationAddressDTO);
                        pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
                        //如果返回信息有error错误，不影响后续流程
                        if(!Detect.notEmpty(fedexValidationAddressResponse.getErrorMsg())){
                            FedexValidationAddressResponse.Output output = fedexValidationAddressResponse.getOutput();
                            if(output!=null){
                                List<FedexValidationAddressResponse.Output.ResolvedAddress> resolvedAddresses = output.getResolvedAddresses();
                                if(Detect.notEmpty(resolvedAddresses)){
                                    FedexValidationAddressResponse.Output.ResolvedAddress resolvedAddress = resolvedAddresses.get(0);
                                    //判断地址类型的 值
                                    String classification = resolvedAddress.getClassification();
                                    FedexValidationAddressClassification fedexValidationAddressClassification = FedexValidationAddressClassification.get(classification);
                                    // 获取到 地址类型信息，且不为UNKNOWN的 则返回正常
                                    if(fedexValidationAddressClassification!=null&&!fedexValidationAddressClassification.equals(FedexValidationAddressClassification.UNKNOWN)){
                                        String mappingChannel = fedexValidationAddressClassification.getMappingChannel();

                                        if(!mappingChannel.equalsIgnoreCase(oldSysOverseasWarehouseChannelDTO.getVirtualChannelCode())) {
                                            List<SysOverseasWarehouseChannelDTO> newSysOverseasWarehouseChannelDTOs = sysOverseasWarehouseChannelDao.selectByWarehouseAndChannelCode(pclOrderB2cSwitchChannelDTO.getShipperWarehouseCode(), mappingChannel, aggregator, ChannelType.FULFILMENT.getType());
                                            if (CollectionUtils.isNotEmpty(newSysOverseasWarehouseChannelDTOs)) {
                                                SysOverseasWarehouseChannelDTO newSysOverseasWarehouseChannelDTO = null;
                                                for(SysOverseasWarehouseChannelDTO overseasWarehouseChannel : newSysOverseasWarehouseChannelDTOs) {
                                                    if(overseasWarehouseChannel.getVirtualChannelName().equalsIgnoreCase(mappingChannel)) {
                                                        newSysOverseasWarehouseChannelDTO = overseasWarehouseChannel;
                                                        break;
                                                    }
                                                }
                                                if(newSysOverseasWarehouseChannelDTO == null) {
                                                    newSysOverseasWarehouseChannelDTO = newSysOverseasWarehouseChannelDTOs.get(0);
                                                }
                                                pclOrderB2cSwitchChannelResultDTO.setChannelId(newSysOverseasWarehouseChannelDTO.getId());
                                                pclOrderB2cSwitchChannelResultDTO.setServiceName(newSysOverseasWarehouseChannelDTO.getVirtualChannelName());
                                                pclOrderB2cSwitchChannelResultDTO.setServiceCode(newSysOverseasWarehouseChannelDTO.getVirtualChannelCode());
                                                pclOrderB2cSwitchChannelResultDTO.setServiceChanged(true);
                                                pclOrderB2cSwitchChannelResultDTO.setChangeServiceBeforeName(oldSysOverseasWarehouseChannelDTO.getVirtualChannelName());
                                                pclOrderB2cSwitchChannelResultDTO.setChangeServiceBeforeCode(oldSysOverseasWarehouseChannelDTO.getVirtualChannelCode());
                                                pclOrderB2cSwitchChannelResultDTO.setAddressClassification(classification);
                                            } else {
                                                pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
                                                pclOrderB2cSwitchChannelResultDTO.setAddressClassification(classification);
                                                log.info("服务进行时未找到目的服务：{},不进行服务切换。", mappingChannel);
                                            }
                                        } else {
                                            pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
                                            pclOrderB2cSwitchChannelResultDTO.setAddressClassification(classification);
                                        }
                                    }else{
                                        pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
                                        pclOrderB2cSwitchChannelResultDTO.setAddressClassification(classification);
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }else{
            pclOrderB2cSwitchChannelResultDTO.setServiceChanged(false);
        }
        return pclOrderB2cSwitchChannelResultDTO;
    }

    @Override
    public List<CostCalculationResultVO> costCalculation(CostCalculationDTO costCalculationDTO) {
        List<CostCalculationResultVO>costCalculationResultVOList=new ArrayList<>();
        Integer aggregator = SessionContext.getContext().getAggregator();
        Integer shipperId = costCalculationDTO.getShipperId();
        String shipperWarehouseCode = costCalculationDTO.getShipperWarehouseCode();
        SysAddress sysAddress=BeanUtils.transform(costCalculationDTO,SysAddress.class);
        SysAddress recipientAddress = sysAddressService.add(sysAddress);
        costCalculationDTO.setAddressId(recipientAddress.getUuid());
        //查询 发货人仓库下生效的服务
        List<SysOverseasWarehouseChannelSimpleDTO> sysOverseasWarehouseChannelList = sysOverseasWarehouseChannelDao.getByWarehouseCodeAndShipperChannelActive(
                shipperId, shipperWarehouseCode, ChannelType.FULFILMENT.getType(), ActiveTypeNew.ACTIVE.getType());
//        List<SysOverseasWarehouseChannelDTO>sysOverseasWarehouseChannelList=sysOverseasWarehouseChannelDao.selectWarehouseCodeChannelByType(shipperWarehouseCode,aggregator,ChannelType.FULFILMENT.getType());
        for (SysOverseasWarehouseChannelSimpleDTO sysOverseasWarehouseChannelDTO : sysOverseasWarehouseChannelList) {
            CostCalculationResultVO costCalculationResultVO=new CostCalculationResultVO();
            costCalculationResultVO.setChannelName(sysOverseasWarehouseChannelDTO.getVirtualChannelName());
            costCalculationResultVO.setChannelId(sysOverseasWarehouseChannelDTO.getId());
            costCalculationDTO.setServiceOption(sysOverseasWarehouseChannelDTO.getServiceOption());
            List<CostCalculationFeeItemVO> feeItemList=new ArrayList<>();
            //出库处理费 系统计算
            CostCalculationFeeItemVO handleFee = getHandleFee(costCalculationDTO, sysOverseasWarehouseChannelDTO);
            feeItemList.add(handleFee);
            //派送费：
            CostCalculationFeeItemVO lastMileFee=getLastMileFee(costCalculationDTO,sysOverseasWarehouseChannelDTO,costCalculationResultVO);
            if(lastMileFee.getBillingWeight()!=null){
                costCalculationResultVO.setLastMileBillingWeight(lastMileFee.getBillingWeight());
            }
            //将派送费价格赋值到服务维度 方便价格排序
            costCalculationResultVO.setLastMileFee(lastMileFee.getFee());
            feeItemList.add(lastMileFee);
            costCalculationResultVO.setFeeItemList(feeItemList);
            costCalculationResultVOList.add(costCalculationResultVO);
        }
        //根据派送费价格 升序排序
        costCalculationResultVOList = costCalculationResultVOList.stream()
                .sorted(Comparator.comparing(CostCalculationResultVO::getLastMileFee,
                        Comparator.nullsLast(BigDecimal::compareTo))).collect(Collectors.toList());
        return costCalculationResultVOList;
    }

    private CostCalculationFeeItemVO getLastMileFee(CostCalculationDTO costCalculationDTO, SysOverseasWarehouseChannelSimpleDTO sysOverseasWarehouseChannel,CostCalculationResultVO costCalculationResultVO) {
        CostCalculationFeeItemVO costCalculationFeeItemVO=new CostCalculationFeeItemVO();
        costCalculationFeeItemVO.setCostTypeName(OverseasWarehouseCostType.VALUE_ADDED_SERVICE_LAST_MILE_FEE.getMessage());
        List<CostCalculationFeeItemDetailVO> lastMileFeeDetailList=new ArrayList<>();
        String shipperWarehouseCode = costCalculationDTO.getShipperWarehouseCode();
        Integer shipperId = costCalculationDTO.getShipperId();
        Long channelId = sysOverseasWarehouseChannel.getId();
        //获取尾程价卡配置
        String dateStr = DateUtils.format(new Date(), DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1);
        List<SysOverseasWarehouseShipperRateDetailDTO> detailDTOS = sysOverseasWarehouseShipperDao.getLastMileRateByShipperAndCode(shipperId, shipperWarehouseCode, channelId, dateStr);
        Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap = detailDTOS.stream().collect(Collectors.groupingBy(p -> p.getRateType()));
        //尾程-基础运费-价卡配置
        List<SysOverseasWarehouseShipperRateDetailDTO> lastMileBaseFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO lastMileRateConfig = Detect.notEmpty(lastMileBaseFeeRateList) ? lastMileBaseFeeRateList.get(0) : null;
        //发货人仓库配置了基本运费价卡 通过系统计算派送费 反之从小包获取
        if (lastMileRateConfig != null && lastMileRateConfig.getRateId() != null) {
            StringBuilder originCurrencyBuilder=new StringBuilder();
            //配置“基本运费”价卡：系统计算
            lastMileFeeDetailList=calculateLastMileFee(costCalculationDTO,rateDetailMap,originCurrencyBuilder,costCalculationResultVO);
            if(Detect.notEmpty(lastMileFeeDetailList)){
                BigDecimal totalFee = lastMileFeeDetailList.stream().map(detailFee->detailFee.getFee()==null?BigDecimal.ZERO:detailFee.getFee()).reduce(BigDecimal.ZERO, BigDecimal::add);
                costCalculationFeeItemVO.setFee(totalFee);
            }else{
                costCalculationFeeItemVO.setFee(null);
            }
            costCalculationFeeItemVO.setCurrency(originCurrencyBuilder.toString());
        }else{
            //未配置“基本运费”价卡：调用小包接口获取费用信息
            SysParty shipper = sysPartyService.getByPartyId(shipperId);
            B2cRateInquiryRequestDTO b2cRateInquiryRequestDTO=convertB2cRateInquiryRequestDTO(costCalculationDTO,sysOverseasWarehouseChannel);
            costCalculationResultVO.setLastMileBillingWeight(b2cRateInquiryRequestDTO.getWeight());
            //调用费用测算接口
            B2cRateInquiryResponseDTO b2cRateInquiryResponseDTO = b2cShipperService.rateInquiry(b2cRateInquiryRequestDTO, shipper);
            costCalculationFeeItemVO.setFee(b2cRateInquiryResponseDTO.getTotalAmount());
            costCalculationFeeItemVO.setCurrency(b2cRateInquiryResponseDTO.getCurrency());
            costCalculationFeeItemVO.setBillingWeight(b2cRateInquiryResponseDTO.getWeight());
            if(!Detect.notEmpty(b2cRateInquiryResponseDTO.getErrorMessage())){
                lastMileFeeDetailList=transToFeeItemDetailList(b2cRateInquiryResponseDTO, lastMileRateConfig);
                //总费用 明细项费用合并 基础运费折扣
                BigDecimal amount = lastMileFeeDetailList.stream().filter(p -> p.getFee() != null).map(p -> p.getFee()).reduce(BigDecimal.ZERO, BigDecimal::add);
                costCalculationFeeItemVO.setFee(amount);
            }
        }
        //过滤掉明细费用为0的
        lastMileFeeDetailList = lastMileFeeDetailList.stream()
                .filter(p -> p.getFee() != null && BigDecimalUtils.isNotZero(p.getFee())).collect(Collectors.toList());
        costCalculationFeeItemVO.setCostCalculationFeeItemDetailVOList(lastMileFeeDetailList);
        return costCalculationFeeItemVO;
    }

    private List<CostCalculationFeeItemDetailVO> transToFeeItemDetailList(B2cRateInquiryResponseDTO b2cRateInquiryResponseDTO, SysOverseasWarehouseShipperRateDetailDTO lastMileRateConfig) {
        List<CostCalculationFeeItemDetailVO>costCalculationFeeItemDetailVOList=new ArrayList<>();
        String currency = b2cRateInquiryResponseDTO.getCurrency();
        //关税
        BigDecimal tax = b2cRateInquiryResponseDTO.getTax();
        CostCalculationFeeItemDetailVO taxFeeItemDetailVO=new CostCalculationFeeItemDetailVO(tax,currency,OverseasWarehouseRateType.DUTY_RATE.getMessage(Locale.SIMPLIFIED_CHINESE),OverseasWarehouseRateType.DUTY_RATE.getMessage(Locale.US));
        costCalculationFeeItemDetailVOList.add(taxFeeItemDetailVO);
        //增值税
        BigDecimal vat = b2cRateInquiryResponseDTO.getVat();
        CostCalculationFeeItemDetailVO vatFeeItemDetailVO=new CostCalculationFeeItemDetailVO(vat,currency,OverseasWarehouseRateType.VAT_RATE.getMessage(Locale.SIMPLIFIED_CHINESE),OverseasWarehouseRateType.VAT_RATE.getMessage(Locale.US));
        costCalculationFeeItemDetailVOList.add(vatFeeItemDetailVO);
        //其他附加费
        BigDecimal surcharge = b2cRateInquiryResponseDTO.getSurcharge();
        CostCalculationFeeItemDetailVO surchargeFeeItemDetailVO=new CostCalculationFeeItemDetailVO(surcharge,currency,LastMileSurchargeType.LAST_MILE_OTHER_SURCHARGE.getMessage(Locale.SIMPLIFIED_CHINESE),LastMileSurchargeType.LAST_MILE_OTHER_SURCHARGE.getMessage(Locale.US));
        costCalculationFeeItemDetailVOList.add(surchargeFeeItemDetailVO);
        //电池附加费
        BigDecimal batteryCharge = b2cRateInquiryResponseDTO.getBatteryCharge();
        CostCalculationFeeItemDetailVO batteryChargeFeeItemDetailVO=new CostCalculationFeeItemDetailVO(batteryCharge,currency,LastMileSurchargeType.BATTERY_SURCHARGE.getMessage(Locale.SIMPLIFIED_CHINESE),LastMileSurchargeType.BATTERY_SURCHARGE.getMessage(Locale.US));
        costCalculationFeeItemDetailVOList.add(batteryChargeFeeItemDetailVO);
        //燃油附加费
        BigDecimal fuelSurcharge = b2cRateInquiryResponseDTO.getFuelSurcharge();
        CostCalculationFeeItemDetailVO fuelSurchargeFeeItemDetailVO=new CostCalculationFeeItemDetailVO(fuelSurcharge,currency,LastMileSurchargeType.LAST_MILE_FUEL_SURCHARGE.getMessage(Locale.SIMPLIFIED_CHINESE),LastMileSurchargeType.LAST_MILE_FUEL_SURCHARGE.getMessage(Locale.US));
        costCalculationFeeItemDetailVOList.add(fuelSurchargeFeeItemDetailVO);
        //基础运费 * etowerOne折扣
        BigDecimal amount = b2cRateInquiryResponseDTO.getAmount();
        Integer lastMileRateDiscount =lastMileRateConfig != null ? lastMileRateConfig.getDiscount() : 0;
        BigDecimal discountedAmount = RateCalculateUtils.calculateDiscountedFee(amount, lastMileRateDiscount);
        CostCalculationFeeItemDetailVO amountFeeItemDetailVO=new CostCalculationFeeItemDetailVO(discountedAmount,currency,OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getMessage(Locale.SIMPLIFIED_CHINESE),OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getMessage(Locale.US));
        amountFeeItemDetailVO.setDiscount(lastMileRateDiscount);
        //费用分区从remark中取
        if (Detect.notEmpty(b2cRateInquiryResponseDTO.getRemarks()) && b2cRateInquiryResponseDTO.getRemarks().startsWith("计费分区")) {
            amountFeeItemDetailVO.setZoneItemName(b2cRateInquiryResponseDTO.getRemarks().replace("计费分区", ""));
        }
        costCalculationFeeItemDetailVOList.add(amountFeeItemDetailVO);
        //偏远地区附加费
        BigDecimal remoteAreaSurcharge = b2cRateInquiryResponseDTO.getRemoteAreaSurcharge();
        CostCalculationFeeItemDetailVO remoteAreaSurchargeFeeItemDetailVO=new CostCalculationFeeItemDetailVO(remoteAreaSurcharge,currency,LastMileSurchargeType.LAST_MILE_REMOTE_AREA_SURCHARGE.getMessage(Locale.SIMPLIFIED_CHINESE),LastMileSurchargeType.LAST_MILE_REMOTE_AREA_SURCHARGE.getMessage(Locale.US));
        costCalculationFeeItemDetailVOList.add(remoteAreaSurchargeFeeItemDetailVO);
        List<B2cRateInquiryForSpecialSurchargeDTO> specialSurchargeList = b2cRateInquiryResponseDTO.getBillDetailSpecialDTOList();
        if(Detect.notEmpty(specialSurchargeList)){
            specialSurchargeList.forEach(specialSurcharge->{
                CostCalculationFeeItemDetailVO specialSurchargeFeeItemDetailVO=new CostCalculationFeeItemDetailVO(specialSurcharge.getAmount(),currency,specialSurcharge.getFeeNameCn(),specialSurcharge.getFeeName());
                costCalculationFeeItemDetailVOList.add(specialSurchargeFeeItemDetailVO);
            });
        }
        return costCalculationFeeItemDetailVOList;
    }

    private B2cRateInquiryRequestDTO convertB2cRateInquiryRequestDTO(CostCalculationDTO costCalculationDTO,SysOverseasWarehouseChannelSimpleDTO sysOverseasWarehouseChannel) {
        B2cRateInquiryRequestDTO b2cRateInquiryRequestDTO=BeanUtils.transform(costCalculationDTO,B2cRateInquiryRequestDTO.class);
        b2cRateInquiryRequestDTO.setServiceCode(sysOverseasWarehouseChannel.getChannelCode());
        b2cRateInquiryRequestDTO.setWeightUnit(WeightUnit.KG.getUnit());
        b2cRateInquiryRequestDTO.setDimensionUnit(DimensionUnit.CM.getUnit());
        b2cRateInquiryRequestDTO.setPostcode(costCalculationDTO.getPostCode());
        b2cRateInquiryRequestDTO.setServiceOption(costCalculationDTO.getServiceOption());
        //传对接仓代码
        SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(costCalculationDTO.getShipperWarehouseCode(), SessionContext.getContext().getAggregator());
        b2cRateInquiryRequestDTO.setFacility(sysOverseasWarehouse != null ? sysOverseasWarehouse.getThirdWarehouseCode() : null);
        List<CostCalculationPackageDTO> costCalculationPackageDTOList = costCalculationDTO.getCostCalculationPackageDTOList();
        CostCalculationPackageDTO costCalculationPackageDTO = costCalculationPackageDTOList.get(0);
        b2cRateInquiryRequestDTO.setWeight(costCalculationPackageDTO.getWeight());
        b2cRateInquiryRequestDTO.setLength(costCalculationPackageDTO.getLength());
        b2cRateInquiryRequestDTO.setWidth(costCalculationPackageDTO.getWidth());
        b2cRateInquiryRequestDTO.setHeight(costCalculationPackageDTO.getHeight());
        List<B2cRateInquiryPiecesDTO>pieces=new ArrayList<>();
        if(costCalculationPackageDTOList.size()>1){
            for (int i=1;i<costCalculationPackageDTOList.size();i++){
                B2cRateInquiryPiecesDTO piecesDTO=BeanUtils.transform(costCalculationPackageDTOList.get(i),B2cRateInquiryPiecesDTO.class);
                pieces.add(piecesDTO);
            }
        }
        b2cRateInquiryRequestDTO.setPieces(pieces);
        return b2cRateInquiryRequestDTO;
    }

    private List<CostCalculationFeeItemDetailVO> calculateLastMileFee(CostCalculationDTO costCalculationDTO, Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap,StringBuilder originCurrencyBuilder,CostCalculationResultVO costCalculationResultVO) {
        List<CostCalculationFeeItemDetailVO>costCalculationFeeItemDetailVOList=new ArrayList<>();
        List<CostCalculationPackageDTO> costCalculationPackageDTOList = costCalculationDTO.getCostCalculationPackageDTOList();
        List<CostCalculationProductDTO> productInfo =new ArrayList<>();
        boolean allPackageHasProduct=true;
        List<CostCalculationProductDTO> costCalculationProductDTOList;
        for(CostCalculationPackageDTO packageDTO:costCalculationPackageDTOList){
            costCalculationProductDTOList = packageDTO.getCostCalculationProductDTOList();
            if(!Detect.notEmpty(costCalculationProductDTOList)){
                allPackageHasProduct=false;
                break;
            }else{
                productInfo.addAll(costCalculationProductDTOList);
            }
        }
        //尾程-基础运费-价卡配置
        List<SysOverseasWarehouseShipperRateDetailDTO> lastMileBaseFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO lastMileRateConfig = Detect.notEmpty(lastMileBaseFeeRateList) ? lastMileBaseFeeRateList.get(0) : null;
        //尾程-附加费-价卡配置
        List<SysOverseasWarehouseShipperRateDetailDTO> lastMileSurchargeFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.LAST_MILE_SURCHARGE_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig = Detect.notEmpty(lastMileSurchargeFeeRateList) ? lastMileSurchargeFeeRateList.get(0) : null;

        //尾程-关税
        List<SysOverseasWarehouseShipperRateDetailDTO> dutyList = rateDetailMap.get(OverseasWarehouseRateType.DUTY_RATE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO dutyRateConfig = Detect.notEmpty(dutyList) ? dutyList.get(0) : null;

        //尾程-增值税
        List<SysOverseasWarehouseShipperRateDetailDTO> vatFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.VAT_RATE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO vatRateConfig = Detect.notEmpty(vatFeeRateList) ? vatFeeRateList.get(0) : null;

        //尾程-保险
        List<SysOverseasWarehouseShipperRateDetailDTO> insuranceFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.INSURANCE_RATE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO insuranceRateConfig = Detect.notEmpty(insuranceFeeRateList) ? insuranceFeeRateList.get(0) : null;
        //基础运费
        CostCalculationFeeItemDetailVO baseFee=new CostCalculationFeeItemDetailVO();
        baseFee.setCostTypeName(OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getMessage(Locale.SIMPLIFIED_CHINESE));
        baseFee.setCostTypeNameEn(OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getMessage(Locale.US));
        ActLastMileRate actLastMileRate;
        if (lastMileRateConfig == null || lastMileRateConfig.getRateId() == null) {
            log.info("未配置尾程价卡，不计算费用");
            baseFee.setFee(null);
            costCalculationFeeItemDetailVOList.add(baseFee);
            return costCalculationFeeItemDetailVOList;
        } else {
            //获取运费价卡信息
            actLastMileRate = actLastMileRateDao.selectByPrimaryKey(lastMileRateConfig.getRateId());
        }
        //>>>>>准备参数
        PclOrderB2c pclOrderB2c = new PclOrderB2c();
        SysAddress sysAddress = new SysAddress();
        sysAddress.setCountry(costCalculationDTO.getCountry());
        sysAddress.setPostCode(costCalculationDTO.getPostCode());
        sysAddress.setState(costCalculationDTO.getState());
        sysAddress.setCity(costCalculationDTO.getCity());
        String recipientAddressId = createAddress(sysAddress);
        pclOrderB2c.getRecipientWarehouseCode();
        pclOrderB2c.setRecipientAddressId(recipientAddressId);
        pclOrderB2c.setShipperWarehouseCode(costCalculationDTO.getShipperWarehouseCode());
        PclOrderB2cOrderDTO pclOrderB2cOrderDTO = new PclOrderB2cOrderDTO();
        List<PclOrderB2cProductDTO> products = Lists.newArrayList();
        List<PclOrderB2cMpsBoxCalcDTO> mpsBoxes = Lists.newArrayList();
        for (int i = 0; i < costCalculationPackageDTOList.size(); i++) {
            CostCalculationPackageDTO parcel = costCalculationPackageDTOList.get(i);
            if (costCalculationPackageDTOList.size() > 1) {
                PclOrderB2cMpsBoxCalcDTO mpsBoxDTO = new PclOrderB2cMpsBoxCalcDTO();
                mpsBoxDTO.setBoxNo(String.valueOf((i + 1)));
                mpsBoxDTO.setBoxLength(parcel.getLength());
                mpsBoxDTO.setBoxWidth(parcel.getWidth());
                mpsBoxDTO.setBoxHeight(parcel.getHeight());
                mpsBoxDTO.setBoxWeight(parcel.getWeight());
                mpsBoxes.add(mpsBoxDTO);
            }
            List<CostCalculationProductDTO> productDTOS = parcel.getCostCalculationProductDTOList();
            //处理商品
            if (Detect.empty(productDTOS)) {
                continue;
            }
            products.addAll(productDTOS.stream().map(productDTO ->{
                PclOrderB2cProductDTO product = new PclOrderB2cProductDTO();
                product.setSku(productDTO.getSku());
                product.setQuantity(productDTO.getProductCount() != null ? productDTO.getProductCount().intValue() : 0);
                product.setProductRealWeight(productDTO.getWeight());
                product.setProductLengthActual(productDTO.getProductLengthActual());
                product.setProductWidthActual(productDTO.getProductWidthActual());
                product.setProductHeightActual(productDTO.getProductHeightActual());
                product.setGrossWeightActual(productDTO.getWeight());
                product.setCurrency(productDTO.getCurrency());
                return product;
            }).collect(Collectors.toList()));
        }
        pclOrderB2cOrderDTO.setProducts(products);
        pclOrderB2cOrderDTO.setMpsBoxes(mpsBoxes);
        //>>>>>
        List<CostCalculationProductDTO> productDTOList =new ArrayList<>();
        //根据条件一，条件二 计算出最终计费重量
        BigDecimal weight;
        if(allPackageHasProduct){
//            weight =calculateFinalWeight(productInfo,actLastMileRate);
            weight = lastMileCalculateFeeService.calculateFinalWeight(pclOrderB2c, pclOrderB2cOrderDTO, actLastMileRate);
            if(weight==null){
                weight=costCalculationDTO.getWeight();
            }
            productDTOList.addAll(productInfo);
        }else{
            //如果不是所有包裹都有商品信息，则将总尺重当做一个商品信息计算计费重
            List<CostCalculationProductDTO> billingProductInfo =new ArrayList<>();
            CostCalculationProductDTO costCalculationProductDTO=new CostCalculationProductDTO();
            costCalculationProductDTO.setWeight(costCalculationDTO.getWeight());
            costCalculationProductDTO.setProductHeightActual(costCalculationDTO.getHeight());
            costCalculationProductDTO.setProductWidthActual(costCalculationDTO.getWidth());
            costCalculationProductDTO.setProductLengthActual(costCalculationDTO.getLength());
            costCalculationProductDTO.setProductCount(1L);
            billingProductInfo.add(costCalculationProductDTO);
//            weight =calculateFinalWeight(billingProductInfo,actLastMileRate);
            weight = lastMileCalculateFeeService.calculateFinalWeight(pclOrderB2c, pclOrderB2cOrderDTO, actLastMileRate);
            if(weight==null){
                weight=costCalculationDTO.getWeight();
            }
            productDTOList.addAll(billingProductInfo);
        }
        AtomicReference<BigDecimal> weightAtomic = new AtomicReference<>(weight);
        costCalculationResultVO.setLastMileBillingWeight(weight);
        //1.基础运费
        LastMileFeeCalcParamDTO calcParamDTO = new LastMileFeeCalcParamDTO();
        calcParamDTO.setCostCalculationDTO(costCalculationDTO);
        calcParamDTO.setLastMileRateConfig(lastMileRateConfig);
        calcParamDTO.setActLastMileRate(actLastMileRate);
        calcParamDTO.setWeightAtomic(weightAtomic);
        calcParamDTO.setOriginCurrencyBuilder(originCurrencyBuilder);
        BigDecimal lastMileBaseFee = getLastMileBaseFee(calcParamDTO, baseFee);
        baseFee.setFee(lastMileBaseFee);
        baseFee.setCurrency(originCurrencyBuilder.toString());
        costCalculationFeeItemDetailVOList.add(baseFee);
        //2.附加费
        /*BigDecimal newSurchargeFee=BigDecimal.ZERO;
        List<CostCalculationFeeItemDetailVO> surchargeFeeList=getSurchargeFee(costCalculationDTO,surchargeRateConfig,weightAtomic,lastMileBaseFee,originCurrencyBuilder,productDTOList);
        if(Detect.notEmpty(surchargeFeeList)){
            costCalculationFeeItemDetailVOList.addAll(surchargeFeeList);
            newSurchargeFee=surchargeFeeList.stream().map(detailFee->detailFee.getFee()==null?BigDecimal.ZERO:detailFee.getFee()).reduce(BigDecimal.ZERO, BigDecimal::add);
        }*/
        ArrayList<ActOverseasWarehouseBillItemDTO> surchargeBillItems = Lists.newArrayList();
        BigDecimal newSurchargeFee = lastMileCalculateFeeService.sumSurchargeFee(pclOrderB2c, surchargeRateConfig,weightAtomic, lastMileBaseFee, surchargeBillItems,
                originCurrencyBuilder, false, pclOrderB2cOrderDTO);
        surchargeBillItems.forEach(billingItem -> {
            CostCalculationFeeItemDetailVO costCalculationFeeItemDetailVO=new CostCalculationFeeItemDetailVO();
            costCalculationFeeItemDetailVO.setCostTypeName(billingItem.getFeeNameCn());
            costCalculationFeeItemDetailVO.setCostTypeNameEn(billingItem.getFeeNameEn());
            costCalculationFeeItemDetailVO.setCurrency(originCurrencyBuilder.toString());
            costCalculationFeeItemDetailVO.setFee(billingItem.getDiscountedAmount());
            costCalculationFeeItemDetailVOList.add(costCalculationFeeItemDetailVO);
        });

        //3.增值税 增值税目前无法计算 需要改版后计算
        CostCalculationFeeItemDetailVO valueAddedTaxFee = getValueAddedTax(lastMileBaseFee, newSurchargeFee, costCalculationDTO, vatRateConfig, originCurrencyBuilder, weightAtomic);
        costCalculationFeeItemDetailVOList.add(valueAddedTaxFee);
        //4.关税
        CostCalculationFeeItemDetailVO dutyFree = getDutyFree(costCalculationDTO, dutyRateConfig, originCurrencyBuilder, productInfo, weightAtomic);
        costCalculationFeeItemDetailVOList.add(dutyFree);
        //5.计算保险费
        CostCalculationFeeItemDetailVO insuranceFee = getInsuranceFee(productInfo, insuranceRateConfig, weightAtomic, baseFee, originCurrencyBuilder);
        costCalculationFeeItemDetailVOList.add(insuranceFee);
        return costCalculationFeeItemDetailVOList;
    }

    private CostCalculationFeeItemDetailVO getInsuranceFee(List<CostCalculationProductDTO> productInfo, SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig, AtomicReference<BigDecimal> weightAtomic, CostCalculationFeeItemDetailVO baseFee, StringBuilder baseCurrencyBuilder) {
        CostCalculationFeeItemDetailVO InsuranceFee=new CostCalculationFeeItemDetailVO();
        InsuranceFee.setCurrency(baseCurrencyBuilder.toString());
        InsuranceFee.setCostTypeName(OverseasWarehouseRateType.INSURANCE_RATE.getMessage(Locale.SIMPLIFIED_CHINESE));
        InsuranceFee.setCostTypeNameEn(OverseasWarehouseRateType.INSURANCE_RATE.getMessage(Locale.US));
        if (surchargeRateConfig == null || Objects.isNull(surchargeRateConfig.getRateId())) {
            log.info("当前保险费未配置燃油附加费,无法计算燃油附加费");
            InsuranceFee.setFee(BigDecimal.ZERO);
            return InsuranceFee;
        }
        Long rateId = surchargeRateConfig.getRateId();
        PclInsuranceConfigDetailResponse pclInsuranceConfigDetailResponse = pclInsuranceConfigService.queryByDetail(rateId);
        if (Objects.isNull(pclInsuranceConfigDetailResponse) || !Detect.notEmpty(pclInsuranceConfigDetailResponse.getInsuranceDetail())) {
            log.info("当前保险费条目详情为空,无法计算保险费");
            InsuranceFee.setFee(BigDecimal.ZERO);
            return InsuranceFee;
        }
        BigDecimal invoiceValue=BigDecimal.ZERO;
        if(Detect.notEmpty(productInfo)){
            invoiceValue=productInfo.stream().map(CostCalculationProductDTO::getInvoiceValue).reduce(BigDecimal.ZERO, BigDecimal::add);
        }
        pclInsuranceConfigDetailResponse.getInsuranceDetail().forEach(e -> {
            e.setValidityStart(e.getStartDate());
            e.setValidityEnd(e.getEndDate());
        });
        Date compareDate = new Date();
        RateCheckUtils.ValidityDateVo validDateItem = RateCheckUtils.getValidDateItem(pclInsuranceConfigDetailResponse.getInsuranceDetail(), compareDate);
        if (validDateItem == null) {
            log.info("保险费条目详情没有在有效期内的item");
            InsuranceFee.setFee(BigDecimal.ZERO);
            return InsuranceFee;
        }
        BigDecimal fuelFee = BigDecimalUtils.ZERO;
        PclInsuranceConfigFindDetailResponse surchargeFuelItemDTO = pclInsuranceConfigDetailResponse.getInsuranceDetail().stream()
                .filter(item -> item.getId().equals(validDateItem.getId()))
                .collect(Collectors.toList()).get(0);
        Integer type = surchargeFuelItemDTO.getType();
        //http://jira.walltechsystem.com/browse/ETOWERONE-3672
        switch (type) {
            case 1:
                fuelFee = surchargeFuelItemDTO.getPremium().divide(new BigDecimal("100"), 4, BigDecimal.ROUND_HALF_UP).multiply(invoiceValue);
                break;
            case 2:
                fuelFee = surchargeFuelItemDTO.getPremium();
                break;
            default:
                log.info("保险费条目类型「{}」不存在，无法计算保险费", type);
                break;
        }
        log.info("保险费:{},计算保险费End", fuelFee);
        //币种转换 基础币种不为空使用基础币种转换 否则使用当前币种作为基础币种
        String targetCurrency;
        if (baseCurrencyBuilder != null && Detect.notEmpty(baseCurrencyBuilder.toString())) {
            targetCurrency = baseCurrencyBuilder.toString();
        } else {
            targetCurrency = "CNY";
            baseCurrencyBuilder.append(targetCurrency);
            log.info("===使用保险费默认币种[{}]作为基础币种====", baseCurrencyBuilder);
        }
        //币种转换
        BigDecimal newInsuranceFee = convertCurrency(fuelFee, "CNY", targetCurrency);
        InsuranceFee.setFee(BigDecimalUtils.covertNullToZero(newInsuranceFee).setScale(3, RoundingMode.HALF_UP));
        InsuranceFee.setCurrency(targetCurrency);
        return InsuranceFee;
    }

    private CostCalculationFeeItemDetailVO getDutyFree(CostCalculationDTO costCalculationDTO, SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig, StringBuilder baseCurrencyBuilder, List<CostCalculationProductDTO> productInfo, AtomicReference<BigDecimal> weightAtomic) {
        CostCalculationFeeItemDetailVO dutyFreeVO=new CostCalculationFeeItemDetailVO();
        dutyFreeVO.setCurrency(baseCurrencyBuilder.toString());
        dutyFreeVO.setCostTypeName(OverseasWarehouseRateType.DUTY_RATE.getMessage(Locale.SIMPLIFIED_CHINESE));
        dutyFreeVO.setCostTypeNameEn(OverseasWarehouseRateType.DUTY_RATE.getMessage(Locale.US));
        if (Objects.isNull(surchargeRateConfig)) {
            log.info("关税ID为空");
            dutyFreeVO.setFee(BigDecimal.ZERO);
            return dutyFreeVO;
        }
        Long surchargeConfigId = surchargeRateConfig.getRateId();
        if (surchargeConfigId == null) {
            dutyFreeVO.setFee(BigDecimal.ZERO);
            return dutyFreeVO;
        }
        //收货人地址
        String recipientAddressId = costCalculationDTO.getAddressId();
        String countryCode = "";
        if (Detect.notEmpty(recipientAddressId)) {
            SysAddress consigneeAddress = addressService.get(recipientAddressId);
            countryCode = consigneeAddress.getCountry();
        }
        if (StringUtils.isBlank(countryCode)) {
            dutyFreeVO.setFee(BigDecimal.ZERO);
            return dutyFreeVO;
        }
        //先查询符合数据的币种
        ActLastMileTaxRequest actLastMileTaxRequest = new ActLastMileTaxRequest();
        actLastMileTaxRequest.setType(LastMileTaxType.TARIFF_TAX_RATE.getCode());
        actLastMileTaxRequest.setCountryCode(countryCode);
        actLastMileTaxRequest.setId(surchargeConfigId);
        actLastMileTaxRequest.setValidityStart(new Date());
        actLastMileTaxRequest.setValidityEnd(DateUtils.getEndOfDay(new Date()));
        List<ActLastMileTaxRateDTO> actLastMileTaxRateDTOList = actLastMileTaxDao.selectRate(actLastMileTaxRequest);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(actLastMileTaxRateDTOList)) {
            log.info("关税:{} 符合日期的关税不存在", JSONObject.toJSONString(actLastMileTaxRequest));
            dutyFreeVO.setFee(BigDecimal.ZERO);
            return dutyFreeVO;
        }
        ActLastMileTaxRateDTO actLastMileTaxRatesDTO = actLastMileTaxRateDTOList.get(0);
        //转成关税的货物总价值
        BigDecimal totalInvoiceValueRequest = getTotalInvoiceValueByScale(productInfo, actLastMileTaxRatesDTO.getCurrency(), 6);
        //先匹配区间，然后匹配币种
        actLastMileTaxRequest.setTaxRateStart(totalInvoiceValueRequest);
        List<ActLastMileTaxRateDTO> actLastMileTaxRateDTOLists = actLastMileTaxDao.selectRate(actLastMileTaxRequest);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(actLastMileTaxRateDTOLists)) {
            log.info("关税:{} 符合币种区间的的关税不存在", JSONObject.toJSONString(actLastMileTaxRequest));
            dutyFreeVO.setFee(BigDecimal.ZERO);
            return dutyFreeVO;
        }
        ActLastMileTaxRateDTO lastMileTaxRateDTO = actLastMileTaxRateDTOLists.get(0);
        String targetCurrency;
        if (baseCurrencyBuilder != null && Detect.notEmpty(baseCurrencyBuilder.toString())) {
            targetCurrency = baseCurrencyBuilder.toString();
        } else {
            targetCurrency = lastMileTaxRateDTO.getCurrency();
            baseCurrencyBuilder.append(targetCurrency);
        }
        //币种转换 基础币种不为空使用基础币种转换 否则使用当前币种作为基础币种
        if (targetCurrency == null) {
            log.info("关税:{} 符合日期的关税币种不存在", JSONObject.toJSONString(actLastMileTaxRequest));
            dutyFreeVO.setFee(BigDecimal.ZERO);
            return dutyFreeVO;
        }
        //关税费率
        BigDecimal newOneSurchargeFee = lastMileTaxRateDTO.getTaxRate();
        //按照关税币种费用计算
        BigDecimal dutyFree = BigDecimalUtils.multiply(newOneSurchargeFee, totalInvoiceValueRequest).divide(new BigDecimal("100"));
        //币种转换 基础币种不为空使用基础币种转换 否则使用当前币种作为基础币种
        if (!baseCurrencyBuilder.toString().equalsIgnoreCase(lastMileTaxRateDTO.getCurrency())) {
            dutyFree = convertCurrency(dutyFree, lastMileTaxRateDTO.getCurrency(), targetCurrency);
        }
        BigDecimal discountedFee = RateCalculateUtils.calculateDiscountedFee(dutyFree, surchargeRateConfig.getDiscount());
        dutyFreeVO.setFee(BigDecimalUtils.covertNullToZero(discountedFee).setScale(3, BigDecimal.ROUND_HALF_UP));
        dutyFreeVO.setCurrency(targetCurrency);
        return dutyFreeVO;
    }

    private BigDecimal getTotalInvoiceValueByScale(List<CostCalculationProductDTO> productInfo, String currency,  int scale) {
        BigDecimal totalInvoiceValue = BigDecimal.ZERO;
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(productInfo)) {
            for (CostCalculationProductDTO product : productInfo) {
                BigDecimal qty = null == product.getProductCount() ? BigDecimal.ZERO : new BigDecimal(product.getProductCount() + "");
                BigDecimal invoiceValue = product.getInvoiceValue();
                if (!Objects.equals(product.getCurrency(), currency)) {
                    invoiceValue = convertCurrencyByScale(product.getInvoiceValue(), product.getCurrency(), currency, scale);
                }
                totalInvoiceValue = totalInvoiceValue.add(invoiceValue.multiply(qty));
            }
        }
        return totalInvoiceValue;
    }

    private BigDecimal convertCurrencyByScale(BigDecimal fee, String oldCurrency, String newCurrency, int scale) {
        // 币种转换
        Map<String, SysChinaBankExchangeRate> rateMap = new HashMap<>();
        return sysChinaBankExchangeRateService.getProductInvoiceValueByScare(fee,
                oldCurrency, newCurrency, rateMap, scale);
    }

    private CostCalculationFeeItemDetailVO getValueAddedTax(BigDecimal baseFee, BigDecimal newSurchargeFee, CostCalculationDTO costCalculationDTO, SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig, StringBuilder baseCurrencyBuilder, AtomicReference<BigDecimal> weightAtomic) {
        CostCalculationFeeItemDetailVO ValueAddedTaxFee=new CostCalculationFeeItemDetailVO();
        ValueAddedTaxFee.setCostTypeName(OverseasWarehouseRateType.VAT_RATE.getMessage(Locale.SIMPLIFIED_CHINESE));
        ValueAddedTaxFee.setCostTypeNameEn(OverseasWarehouseRateType.VAT_RATE.getMessage(Locale.US));
        ValueAddedTaxFee.setCurrency(baseCurrencyBuilder.toString());
        if (!BigDecimalUtils.isNotNullAndNotZero(baseFee) && !BigDecimalUtils.isNotNullAndNotZero(newSurchargeFee)) {
            log.info("基础运费和增值税都为空");
            ValueAddedTaxFee.setFee(BigDecimal.ZERO);
            return ValueAddedTaxFee;
        }
        if (Objects.isNull(surchargeRateConfig)) {
            log.info("增值税为空");
            ValueAddedTaxFee.setFee(BigDecimal.ZERO);
            return ValueAddedTaxFee;
        }
        Long surchargeConfigId = surchargeRateConfig.getRateId();
        if (surchargeConfigId == null) {
            log.info("增值税ID为空");
            ValueAddedTaxFee.setFee(BigDecimal.ZERO);
            return ValueAddedTaxFee;
        }
        //收货人地址
        String recipientAddressId = costCalculationDTO.getAddressId();
        String countryCode = "";
        if (Detect.notEmpty(recipientAddressId)) {
            SysAddress consigneeAddress = addressService.get(recipientAddressId);
            countryCode = consigneeAddress.getCountry();
        }
        if (StringUtils.isBlank(countryCode)) {
            ValueAddedTaxFee.setFee(BigDecimal.ZERO);
            return ValueAddedTaxFee;
        }
        //币种转换 基础币种不为空使用基础币种转换 否则使用当前币种作为基础币种
        BigDecimal totalInvoiceValue = BigDecimalUtils.add(baseFee, newSurchargeFee);
        ActLastMileTaxRequest actLastMileTaxRequest = new ActLastMileTaxRequest();
        actLastMileTaxRequest.setType(LastMileTaxType.LAST_MILE_VAT_RATE.getCode());
        actLastMileTaxRequest.setCountryCode(countryCode);
        actLastMileTaxRequest.setId(surchargeConfigId);
        actLastMileTaxRequest.setValidityStart(new Date());
        actLastMileTaxRequest.setValidityEnd(DateUtils.getEndOfDay(new Date()));
        ActLastMileTaxRateDTO actLastMileTaxRateDTO = null;
        ActLastMileTaxRateDTO actLastMileTaxRateTongYongDTO = null;

        List<ActLastMileTaxRateDTO> actLastMileTaxRateDTOList = actLastMileTaxDao.selectRate(actLastMileTaxRequest);
        for (ActLastMileTaxRateDTO actLastMileTaxRatesDTO : actLastMileTaxRateDTOList) {
            Integer weights = checkCountryAndZoneItemInfo(costCalculationDTO, countryCode, actLastMileTaxRatesDTO.getPartitionNameId());
            if (weights == 1) {
                actLastMileTaxRateDTO = actLastMileTaxRatesDTO;
                break;
            }
            if (null == actLastMileTaxRatesDTO.getPartitionNameId()) {
                actLastMileTaxRateTongYongDTO = actLastMileTaxRatesDTO;
            }
        }
        if (Objects.isNull(actLastMileTaxRateDTO) && Objects.isNull(actLastMileTaxRateTongYongDTO)) {
            log.info("增值税:{} 符合日期的增值税不存在", JSONObject.toJSONString(actLastMileTaxRequest));
            ValueAddedTaxFee.setFee(BigDecimal.ZERO);
            return ValueAddedTaxFee;
        }
        if (Objects.nonNull(actLastMileTaxRateTongYongDTO)) {
            actLastMileTaxRateDTO = actLastMileTaxRateTongYongDTO;
        }
        String targetCurrency;
        if (baseCurrencyBuilder != null && Detect.notEmpty(baseCurrencyBuilder.toString())) {
            targetCurrency = baseCurrencyBuilder.toString();
        } else {
            targetCurrency = actLastMileTaxRateDTO.getCurrency();
            baseCurrencyBuilder.append(targetCurrency);
        }
        if (targetCurrency == null) {
            log.info("增值税:{} 符合日期的增值税币种不存在", JSONObject.toJSONString(actLastMileTaxRequest));
            ValueAddedTaxFee.setFee(BigDecimal.ZERO);
            return ValueAddedTaxFee;
        }
        ValueAddedTaxFee.setCurrency(targetCurrency);
        //增值税费率
        BigDecimal newOneSurchargeFee = actLastMileTaxRateDTO.getTaxRate();
        //增值税费用
        BigDecimal valueAddedTax = BigDecimalUtils.multiply(newOneSurchargeFee, totalInvoiceValue).divide(new BigDecimal("100"), 4, RoundingMode.HALF_UP);
        BigDecimal discountedFee = RateCalculateUtils.calculateDiscountedFee(valueAddedTax, surchargeRateConfig.getDiscount());
        ValueAddedTaxFee.setFee(discountedFee);
        return ValueAddedTaxFee;
    }

    private List<CostCalculationFeeItemDetailVO> getSurchargeFee(CostCalculationDTO costCalculationDTO, SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig, AtomicReference<BigDecimal> weightAtomic,BigDecimal baseFee,StringBuilder baseCurrencyBuilder,List<CostCalculationProductDTO> productDTOList) {
        List<CostCalculationFeeItemDetailVO>costCalculationFeeItemDetailVOList=new ArrayList<>();
        String targetCurrency;
        BigDecimal oneSurchargeFee = BigDecimalUtils.ZERO;
        BigDecimal totalSurchargeFee = BigDecimal.ZERO;
        String surchargeCurrency;
        if (surchargeRateConfig == null) {
            log.info("未配置附加费");
            return null;
        }
        Long surchargeConfigId = surchargeRateConfig.getRateId();
        ActLastMileSurchargeConfig surchargeConfig = lastMileSurchargeConfigDao.selectByPrimaryKey(surchargeConfigId);
        if (surchargeConfig == null || !Objects.equals(surchargeConfig.getActive(), ActiveTypeNew.ACTIVE.getType())) {
            log.info("附加费ID:{}不存在或者已经失效", surchargeConfigId);
           return null;
        }
        List<ActLastMileSurchargeConfigItem> configItems = lastMileSurchargeConfigItemDao.getByConfigId(surchargeConfigId);
        if (!Detect.notEmpty(configItems)) {
            log.info("附加费ID:{}未配置任何的附加费方案,无法计算附加费", surchargeConfigId);
            return null;
        }
        surchargeCurrency = surchargeConfig.getCurrency();
        if (surchargeCurrency == null) {
            surchargeCurrency = RateCalculateUtils.CNY_CURRENCY;
        }
        //遍历所有的附加费关联的所有附加费方案计算费用
        Map<Short, List<ActLastMileSurchargeConfigItem>> typeMap = configItems.stream().collect(Collectors.groupingBy(p -> p.getSurchargeType()));
        //获取所有未失效的附加费方案，然后筛选
        List<Long> surchargeAllIds = configItems.stream().map(ActLastMileSurchargeConfigItem::getSurchargeId).collect(Collectors.toList());
        List<ActLastMileSurcharge> actAllLastMileSurcharges = actLastMileSurchargeDao.selectByIds(Joiner.on(",").join(surchargeAllIds));
        if (actAllLastMileSurcharges.isEmpty()) {
            return null;
        }
        BigDecimal remoteAreaFee = BigDecimal.ZERO;
        Map<Long, ActLastMileSurcharge> mileSurchargeMap = actAllLastMileSurcharges.stream()
                .filter(e -> Objects.equals(e.getActive(), ActiveTypeNew.ACTIVE.getType()))
                .collect(Collectors.toMap(ActLastMileSurcharge::getId, Function.identity()));
        for (Map.Entry<Short, List<ActLastMileSurchargeConfigItem>> entry : typeMap.entrySet()) {
            Short code = entry.getKey();
            Long surchargeId = entry.getValue().get(0).getSurchargeId();
            if (Objects.isNull(mileSurchargeMap.get(surchargeId))) {
                continue;
            }
            LastMileSurchargeType typeEnum = LastMileSurchargeType.getByCode(code);
            switch (typeEnum.getType()) {
                case 1:
                    //计算 偏远地区附加费
                    oneSurchargeFee = getRemoteAreaFee(costCalculationDTO,surchargeId, weightAtomic.get());
                    remoteAreaFee=oneSurchargeFee;
                    break;
                case 2:
                    //计算 安全管理费,其他附加费,使用相同的计算逻辑
                    oneSurchargeFee = getOtherFee(surchargeId,weightAtomic.get(),baseFee);
                    break;
                case 3:
                    //计算 燃油附加费
                    oneSurchargeFee = getFuelFee(surchargeId,weightAtomic.get(),baseFee,remoteAreaFee);
                    break;
                case 6:
                    //计算自定义附加费
                    oneSurchargeFee = getCustomFee(costCalculationDTO, surchargeId, weightAtomic.get(), productDTOList);
                default:
                    break;
            }
            //币种转换 基础币种不为空使用基础币种转换 否则使用当前币种作为基础币种
            if (baseCurrencyBuilder != null && Detect.notEmpty(baseCurrencyBuilder.toString())) {
                targetCurrency = baseCurrencyBuilder.toString();
            } else {
                targetCurrency = surchargeConfig.getCurrency();
            }
            CostCalculationFeeItemDetailVO costCalculationFeeItemDetailVO=new CostCalculationFeeItemDetailVO();
            costCalculationFeeItemDetailVO.setCostTypeName(typeEnum.getMessage(Locale.SIMPLIFIED_CHINESE));
            costCalculationFeeItemDetailVO.setCostTypeNameEn(typeEnum.getMessage(Locale.US));
            costCalculationFeeItemDetailVO.setCurrency(targetCurrency);
            if(oneSurchargeFee==null){
                costCalculationFeeItemDetailVO.setFee(null);
                costCalculationFeeItemDetailVOList.add(costCalculationFeeItemDetailVO);
                return costCalculationFeeItemDetailVOList;
            }
            //币种转换成基础币种
            final BigDecimal newOneSurchargeFee = convertCurrency(oneSurchargeFee, surchargeCurrency, targetCurrency);
            //计算折扣后费用金额
            BigDecimal discountedFee = RateCalculateUtils.calculateDiscountedFee(newOneSurchargeFee, surchargeRateConfig.getDiscount());
            //总附加费(折扣后费用)
            totalSurchargeFee = totalSurchargeFee.add(discountedFee);
            costCalculationFeeItemDetailVO.setFee(discountedFee);
            costCalculationFeeItemDetailVOList.add(costCalculationFeeItemDetailVO);
        }
        return costCalculationFeeItemDetailVOList;
    }

    private BigDecimal getFuelFee(Long surchargeId, BigDecimal weight, BigDecimal baseFee, BigDecimal remoteAreaFee) {
        log.info("计算燃油附加费Start");
        if (surchargeId == null) {
            log.info("当前附加费未配置燃油附加费,无法计算燃油附加费");
            return null;
        }
        List<ActLastMileSurchargeFuelItemDTO> surchargeFuelItemDTOList = lastMileSurchargeFuelItemDao.selectBySurchargeId(surchargeId);
        if (!Detect.notEmpty(surchargeFuelItemDTOList)) {
            log.info("当前燃油附加费条目详情为空,无法计算燃油附加费");
            return null;
        }
        Date compareDate =new Date();
        RateCheckUtils.ValidityDateVo validDateItem = RateCheckUtils.getValidDateItem(surchargeFuelItemDTOList, compareDate);
        if (validDateItem == null) {
            log.info("燃油附加费ID:{}, 条目详情 没有在有效期内的item", surchargeId);
            return null;
        }
        BigDecimal fuelFee = BigDecimalUtils.ZERO;
        ActLastMileSurchargeFuelItemDTO surchargeFuelItemDTO = surchargeFuelItemDTOList.stream()
                .filter(item -> item.getId().equals(validDateItem.getId()))
                .collect(Collectors.toList()).get(0);
        Short unit = surchargeFuelItemDTO.getUnit();
        BigDecimal surchargeFee = surchargeFuelItemDTO.getSurchargeFee();
        LastMileSurchargeBillingUnitType unitType = LastMileSurchargeBillingUnitType.getByCode(unit);
        switch (unitType) {
            case KG:
                fuelFee = weight.multiply(surchargeFee);
                break;
            case PERCENT:
                if (surchargeFee != null) {
                    BigDecimal percent = surchargeFee.divide(new BigDecimal(100), 4, BigDecimal.ROUND_HALF_UP);
                    fuelFee = BigDecimalUtils.covertNullToZero(baseFee).add(BigDecimalUtils.covertNullToZero(remoteAreaFee)).multiply(percent);
                }
                break;
            case ORDER:
                fuelFee = surchargeFee;
                break;
            default:
                log.info("燃油附加费条目 附加费单位「{}」不存在，无法计算燃油附加费", unit);
                break;
        }
        log.info("燃油附加费:{},计算燃油附加费End", fuelFee);
        return fuelFee;
    }

    private BigDecimal getOtherFee(Long surchargeId, BigDecimal weight, BigDecimal baseFee) {
        log.info("计算其他类型附加费Start");
        if (surchargeId == null) {
            log.info("当前附加费未配置其他附加费,无法计算其他附加费");
            return null;
        }
        List<ActLastMileSurchargeFuelItemDTO> surchargeFuelItemDTOList = lastMileSurchargeFuelItemDao.selectBySurchargeId(surchargeId);
        if (!Detect.notEmpty(surchargeFuelItemDTOList)) {
            log.info("当前其他附加费条目详情为空,无法计算其他附加费");
            return null;
        }
        Date compareDate =new Date();
        RateCheckUtils.ValidityDateVo validDateItem = RateCheckUtils.getValidDateItem(surchargeFuelItemDTOList, compareDate);
        if (validDateItem == null) {
            return null;
        }
        BigDecimal fuelFee = BigDecimalUtils.ZERO;
        ActLastMileSurchargeFuelItemDTO surchargeFuelItemDTO = surchargeFuelItemDTOList.stream()
                .filter(item -> item.getId().equals(validDateItem.getId()))
                .collect(Collectors.toList()).get(0);
        Short unit = surchargeFuelItemDTO.getUnit();
        BigDecimal surchargeFee = surchargeFuelItemDTO.getSurchargeFee();
        LastMileSurchargeBillingUnitType unitType = LastMileSurchargeBillingUnitType.getByCode(unit);
        switch (unitType) {
            case KG:
                fuelFee = weight.multiply(surchargeFee);
                break;
            case PERCENT:
                if (surchargeFee != null) {
                    BigDecimal percent = surchargeFee.divide(new BigDecimal(100), 4, BigDecimal.ROUND_HALF_UP);
                    fuelFee = BigDecimalUtils.covertNullToZero(baseFee).multiply(percent);
                }
                break;
            case ORDER:
                fuelFee = surchargeFee;
                break;
            default:
                log.info("其他附加费条目 附加费单位「{}」不存在，无法计算其他附加费", unit);
                break;
        }
        log.info("其他附加费:{},计算其他附加费End", fuelFee);
        return fuelFee;
    }

    private BigDecimal convertCurrency(BigDecimal fee, String oldCurrency, String newCurrency) {
        if (fee == null || fee.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        // 币种转换
        Map<String, SysChinaBankExchangeRate> rateMap = new HashMap<>();
        return sysChinaBankExchangeRateService.getProductInvoiceValue(fee,
                oldCurrency, newCurrency, rateMap);
    }

    public BigDecimal getRemoteAreaFee(CostCalculationDTO costCalculationDTO,  Long remoteAreaSurchargeId, BigDecimal weight) {
        log.info("计算偏远地区附加费Start");
        if (remoteAreaSurchargeId == null) {
            log.info("当前附加费未配置偏远地区附加费,无法计算偏远地区附加费");
            return null;
        }
        //获取再有效期内的选项
        List<ActLastMileSurchargeFuelItemDTO> surchargeFuelItemDTOS = lastMileSurchargeFuelItemDao.selectBySurchargeId(remoteAreaSurchargeId);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(surchargeFuelItemDTOS)) {
            return null;
        }
        // 使用订单出库时间匹配价卡
        Date compareDate = new Date();
        List<Long> itemIds = surchargeFuelItemDTOS.stream().filter(e -> {
            if (Objects.isNull(e.getValidityStart())) {
                return false;
            }
            if (e.getValidityStart().after(compareDate)) {
                return false;
            }
            if (e.getValidityEnd() == null) {
                return true;
            }
            if (compareDate.before(e.getValidityEnd())) {
                return true;
            }
            return false;
        }).map(ActLastMileSurchargeFuelItemDTO::getId).collect(Collectors.toList());
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(itemIds)) {
            return null;
        }
        List<ActLastMileSurchargeItemDetailInfo> remoteAreaItemInfoList = lastMileSurchargeFuelItemDao.selectByItemsIds(itemIds);
        if (!Detect.notEmpty(remoteAreaItemInfoList)) {
            log.info("当前偏远地区附加费条目详情为空,无法计算偏远地区附加费");
            return null;
        }
        BigDecimal remoteAreaFee = new BigDecimal(0);

        log.info("当前所有条目:{}", JSON.toJSONString(remoteAreaItemInfoList));

        //存下只是国家/地区 和 分区名称匹配的价卡条目 用于计算按票的时候计算续重费
        ArrayList<ActLastMileSurchargeItemDetailInfo> countryZoneMatchedList = Lists.newArrayList();
        for (ActLastMileSurchargeItemDetailInfo item : remoteAreaItemInfoList) {
            //判断国家/地区 和 分区名称是否匹配
            Integer weights = checkCountryAndZoneItemInfo(costCalculationDTO, item.getCountryCode(), item.getZoneItemId());
            if (weights == -1) {
                continue;
            }

            //匹配重量段
            BigDecimal weightStart = item.getWeightStart();
            BigDecimal weightEnd = item.getWeightEnd();
            if (weightStart != null && BigDecimalUtils.le(weight, weightStart)) {
                continue;
            }
            if (weightEnd != null && BigDecimalUtils.gt(weight, weightEnd)) {
                continue;
            }
            item.setWeights(weights);
            countryZoneMatchedList.add(item);
        }
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(countryZoneMatchedList)) {
            countryZoneMatchedList.sort(Comparator.comparing(ActLastMileSurchargeItemDetailInfo::getWeights));
            //偏远地区附加费规则变更：ETOWERONE-3641  计费重量*计费单价（KG）+计费单价（票）
            ActLastMileSurchargeItemDetailInfo selectedItem = countryZoneMatchedList.get(0);
            BigDecimal weightPrice = BigDecimalUtils.covertNullToZero(selectedItem.getWeightPrice());
            BigDecimal orderPrice = BigDecimalUtils.covertNullToZero(selectedItem.getOrderPrice());
            remoteAreaFee = weight.multiply(weightPrice).add(orderPrice);
        }
        log.info("偏远地区附加费:{},计算偏远地区附加费End", remoteAreaFee);
        return remoteAreaFee;
    }

    /**
     * ****************费用测算不做 ETOWERONE-7762
     */
    public Integer checkCountryAndZoneItemInfo(CostCalculationDTO costCalculationDTO, String countryCode, Long zoneItemId) {
        //收货人地址
        SysAddress consigneeAddress = addressService.get(costCalculationDTO.getAddressId());
        if (consigneeAddress == null) {
            return -1;
        }
        //2.校验国家，分区 国家和分区
        String receiptCountry = consigneeAddress.getCountry();
        boolean countryCheck = false;
        boolean zoneItemCheck = false;
        if (StringUtils.isNotEmpty(countryCode)) {
            if (!StringUtils.equalsIgnoreCase(countryCode, receiptCountry)) {
                return -1;
            } else {
                countryCheck = true;
            }
        }
        if (!(zoneItemId == null || zoneItemId <= 0)) {
            //使用已有的校验分区逻辑
            List<SysZoneItemDetails> zones = sysZoneItemDetailsDao.queryDetailsByZoneItemId(zoneItemId);
            zoneItemCheck = ZoneCheckUtils.isRateZoneSuit(zones, consigneeAddress,null);
            if (!zoneItemCheck) {
                return -1;
            }
        }
        //国家和分区同时校验通过才算匹配
        if (countryCheck && zoneItemCheck) {
            return 1;
        }
        if (zoneItemCheck) {
            return 2;
        }
        if (countryCheck) {
            return 3;
        }
        return 4;
    }

    private BigDecimal getLastMileBaseFee(LastMileFeeCalcParamDTO calcParamDTO, CostCalculationFeeItemDetailVO baseFee) {
        CostCalculationDTO costCalculationDTO = calcParamDTO.getCostCalculationDTO();
        SysOverseasWarehouseShipperRateDetailDTO lastMileRateConfig = calcParamDTO.getLastMileRateConfig();
        ActLastMileRate actLastMileRate = calcParamDTO.getActLastMileRate();
        AtomicReference<BigDecimal> weight = calcParamDTO.getWeightAtomic();
        StringBuilder originCurrencyBuilder = calcParamDTO.getOriginCurrencyBuilder();
        //1.获取当前渠道的所有代发出库的费率类型
        Date compareDate = new Date();
        List<ActRateAndItemDTO> rateItemList = actLastMileRateHelper.getRateInfoByRateIdId(actLastMileRate.getId(), compareDate != null ? compareDate.getTime() : null).getData();
        if (!Detect.notEmpty(rateItemList)) {
            log.info("获取到的基本运费有效期区间为空");
            return null;
        }
        originCurrencyBuilder.append(actLastMileRate.getCurrency());
//        //2.已经拿到了计费重 weight,进行规则匹配
//        ActRateAndItemDTO actRateAndItem = rateItemList.get(0);
//        ChargeDataType chargeDataType = ChargeDataType.getByType(actRateAndItem.getChargeDataType());
//        weight.set(chargeDataType.handle(weight.get()));
        BigDecimal fee;
        ActLastMileRateCalculateDTO calculateDTO = new ActLastMileRateCalculateDTO();
        calculateDTO.setChargeNum(weight.get());
        calculateDTO.setConsigneeAddressId(costCalculationDTO.getAddressId());
        calculateDTO.setWarehouceCode(null);
        calculateDTO.setActRateAndItemList(rateItemList);

    //        ActRateDetailVo chargeAmountResult = actLastMileRateHelper.calculateChargeAmount(calculateDTO).getData();
        ActRateDetailVo hitRateDetail = actRateHelper.matchFreightRateItemDetail(calculateDTO);
        if (hitRateDetail == null) {
            log.info("已命中有效期区间下价卡条目未匹配上");
            return null;
        }
        baseFee.setZoneItemName(hitRateDetail.getZoneItemName());
        hitRateDetail.setCarryRule(actLastMileRate.getCarryRule());
        fee = actRateHelper.calcB2cBasicFreightFee(hitRateDetail, weight.get());
        if (BigDecimalUtils.isZero(fee)) {
            log.info("根据匹配上的重量区间，计算的计费金额为空或者0");
        }
        //5.计算折扣后费用金额
        return RateCalculateUtils.calculateDiscountedFee(fee, lastMileRateConfig.getDiscount());
    }

    private BigDecimal calculateFinalWeight(List<CostCalculationProductDTO> productInfo, ActLastMileRate actLastMileRate) {
        BigDecimal totalOriginWeight = BigDecimalUtils.ZERO;
        BigDecimal totalVolume = BigDecimalUtils.ZERO;
        BigDecimal totalGrith;
        BigDecimal finalWeight;
        if (!Detect.notEmpty(productInfo)) {
            return null;
        }
        //遍历出库商品 计算总重量, 总体积， 总周长
        for (CostCalculationProductDTO b2cProduct : productInfo) {
            Long quantity = b2cProduct.getProductCount() != null ? b2cProduct.getProductCount() : 0;
            BigDecimal qty = new BigDecimal(quantity);
            BigDecimal weightActual = b2cProduct.getWeight();
            BigDecimal weight = weightActual.multiply(qty);
            totalOriginWeight = totalOriginWeight.add(weight);
            //计算体积
            BigDecimal length = BigDecimalUtils.covertNullToZero(b2cProduct.getProductLengthActual());
            BigDecimal width = BigDecimalUtils.covertNullToZero(b2cProduct.getProductWidthActual());
            BigDecimal height = BigDecimalUtils.covertNullToZero(b2cProduct.getProductHeightActual());
            BigDecimal volume = length.multiply(width).multiply(height).multiply(qty);
            totalVolume = totalVolume.add(volume);
        }
        //根据箱子信息计算总周长
        totalGrith = calculateGrith(productInfo);
        log.info("配置了尾程运价价卡，最终计费重量通过条件一和条件二规则决定");
        //重量的条件判断参数
        BigDecimal heavyBulkyRatio = BigDecimalUtils.covertNullToZero(actLastMileRate.getHeavyBulkyRatio());
        BigDecimal weightMultiple = BigDecimalUtils.covertNullToZero(actLastMileRate.getWeightMultiple());
        BigDecimal lwhPlusThreshold = BigDecimalUtils.covertNullToZero(actLastMileRate.getLwhPlusThreshold());

        Boolean conditionOneActive = actLastMileRate.getConditionOneActive();
        conditionOneActive = conditionOneActive != null ? conditionOneActive : false;
        Boolean conditionTwoActive = actLastMileRate.getConditionTwoActive();
        conditionTwoActive = conditionTwoActive != null ? conditionTwoActive : false;
        log.info("泡重比:{},重量倍数:{}, 周长阈值:{}" , heavyBulkyRatio, weightMultiple, lwhPlusThreshold);
        log.info("计算得到的周长:{},体积:{}" , totalGrith, totalVolume);
        log.info("条件一:{},条件二:{}" , conditionOneActive, conditionTwoActive);
        finalWeight = totalOriginWeight;
        //规则:ETOWERB2B-2335
        //√ 条件1&2没勾，按实际重量匹配重量段计算；
        //√ 条件1勾、2没勾，按照体积比公式计算，体积重和实际重，哪个重，按哪个匹配重量段计算；
        //√ 条件1&2都勾，且都命中，按照体积比公式计算，体积重和实际重，哪个重，按哪个匹配重量段计算；
        //√ 条件1&2都勾，只命中1个，按实际重量匹配重量段计算；(理解为只要条件二不成立，就使用实际重量，也不用去判断条件一)
        if (!conditionOneActive) {
            log.info("条件一为禁用状态,最终计费重量为实际重量");
            return totalOriginWeight;
        } else {
            log.info("条件一为启用状态");
            //先判断条件二
            boolean needJudgeOne = true;
            if (conditionTwoActive) {
                log.info("条件二为启用状态");
                if (!BigDecimalUtils.gt(totalGrith, lwhPlusThreshold)) {
                    log.info("条件二不成立,最终计费重量为实际重量");
                    needJudgeOne = false;
                    finalWeight = totalOriginWeight;
                }
            }
            if (!conditionTwoActive || needJudgeOne) {
                log.info("条件二为禁用状态或者条件二已经成立，将根据条件一的判断结果作为最终计费重量...");
                if (BigDecimalUtils.isZero(heavyBulkyRatio)) {
                    log.info("条件一参数[重泡比]除数不能为0,将使用实际重量为计费重量");
                    finalWeight = totalOriginWeight;
                    return finalWeight;
                }
                BigDecimal value1 = totalVolume.divide(heavyBulkyRatio, 6, BigDecimal.ROUND_HALF_UP);
                BigDecimal value2 = weightMultiple.multiply(totalOriginWeight);
                if (BigDecimalUtils.gt(value1, value2)) {
                    finalWeight = value1;
                } else {
                    finalWeight = totalOriginWeight;
                }
            }
        }
        log.info("最终计费重量:{}", finalWeight);
        return finalWeight;
    }

    private BigDecimal calculateGrith(List<CostCalculationProductDTO> productInfo) {
        for (CostCalculationProductDTO product : productInfo) {
            ArrayList<BigDecimal> lwhList = compareLWH(product.getProductLengthActual(), product.getProductWidthActual(), product.getProductHeightActual());
            product.setProductLengthActual(lwhList.get(0));
            product.setProductWidthActual(lwhList.get(1));
            product.setProductHeightActual(lwhList.get(2));
        }
        BigDecimal maxLength = productInfo.stream().map(CostCalculationProductDTO::getProductLengthActual).max(BigDecimal::compareTo).get();
        BigDecimal maxWidth = productInfo.stream().map(CostCalculationProductDTO::getProductWidthActual).max(BigDecimal::compareTo).get();
        BigDecimal sumHeight = productInfo.stream().map(CostCalculationProductDTO::getProductHeightActual).reduce(BigDecimal.ZERO, BigDecimal::add);
        return maxLength.add(maxWidth).add(sumHeight);
    }

    private ArrayList<BigDecimal> compareLWH(BigDecimal length, BigDecimal width, BigDecimal height) {
        BigDecimal tmp;
        length = BigDecimalUtils.covertNullToZero(length);
        width = BigDecimalUtils.covertNullToZero(width);
        height = BigDecimalUtils.covertNullToZero(height);
        if (height.compareTo(width) == 1) {
            tmp = height;
            height = width;
            width = tmp;
        }
        if (height.compareTo(length) == 1) {
            tmp = length;
            length = height;
            height = tmp;
        }
        if (width.compareTo(length) == 1) {
            tmp = width;
            width = length;
            length = tmp;
        }
        ArrayList<BigDecimal> list = Lists.newArrayList();
        list.add(length);
        list.add(width);
        list.add(height);
        return list;
    }

    /**
     *
     * @param costCalculationDTO
     * @param sysOverseasWarehouseChannel
     * @return null 为未找到价卡等
     */
    @Override
    public CostCalculationFeeItemVO getHandleFee(CostCalculationDTO costCalculationDTO,SysOverseasWarehouseChannelSimpleDTO sysOverseasWarehouseChannel) {
        CostCalculationFeeItemVO costCalculationFeeItemVO=new CostCalculationFeeItemVO();
        costCalculationFeeItemVO.setCostTypeName(OverseasWarehouseRateType.HANDLE.getMessage());
        Integer shipperId = costCalculationDTO.getShipperId();
        String shipperWarehouseCode = costCalculationDTO.getShipperWarehouseCode();
        BigDecimal totalAmount =BigDecimal.ZERO;
        List<CostCalculationPackageDTO> costCalculationPackageDTOList = costCalculationDTO.getCostCalculationPackageDTOList();
        List<CostCalculationProductDTO>calculationProductDTOList=new ArrayList<>();
        boolean allPackageHasProduct=true;
        for(CostCalculationPackageDTO packageDTO:costCalculationPackageDTOList){
            List<CostCalculationProductDTO> costCalculationProductDTOList = packageDTO.getCostCalculationProductDTOList();
            if(!Detect.notEmpty(costCalculationProductDTOList)){
                allPackageHasProduct=false;
            }else{
                calculationProductDTOList.addAll(costCalculationProductDTOList);
            }
        }
        if(!allPackageHasProduct){
            costCalculationFeeItemVO.setFee(null);
            return costCalculationFeeItemVO;
        }
        SysOverseasWarehouseShipperRateDetailDTO handleRateDTO = sysOverseasWarehouseShipperService.getNonLastMileRateConfigInfo(shipperId, shipperWarehouseCode, OverseasWarehouseRateType.HANDLE);
        if(handleRateDTO==null){
            costCalculationFeeItemVO.setFee(null);
            return costCalculationFeeItemVO;
        }
        SysOverseasWarehouseShipperRateDetailDTO lastMileBaseRateInfo = sysOverseasWarehouseShipperService.getNonLastMileRateConfigInfo(shipperId, shipperWarehouseCode, OverseasWarehouseRateType.LAST_MILE_BASE_FEE);

        Long handleId = handleRateDTO.getRateId();
        String handleCurrency = null;
        ActOverseasWarehouseRate rate;
        Date today = new Date();
        if (handleId == null) {
            costCalculationFeeItemVO.setFee(null);
            return costCalculationFeeItemVO;
        }else{
            // 获取价卡信息得到币种
            rate = actStorageFeeRateDao.selectByPrimaryKey(handleId);
            if (rate != null) {
                handleCurrency = rate.getCurrency();
            }
            if (StringUtils.isEmpty(handleCurrency)) {
                handleCurrency = "CNY";
            }
        }
        ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, today);
        //未配置价卡则费用为0
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        if (Detect.notEmpty(actHandleReturnDetails)) {
            //计费明细
            List<ActOverseasWarehouseBillingItemHandling> billingItemList = Lists.newArrayList();
            if (rate != null && rate.getBillingType() != null && rate.getBillingType().equals(OverseasWarehouseRateBillingType.ORDER.getType())) {
                List<B2cProductInfo> productInfos=BeanUtils.transform(calculationProductDTOList,B2cProductInfo.class);
                totalAmount = calculateOrderFee(productInfos, rateDetail, rate, handleRateDTO, billingItemList);
            } else {
                for (CostCalculationProductDTO calculationProductDTO : calculationProductDTOList) {
                    B2cProductInfo productInfo=BeanUtils.transform(calculationProductDTO,B2cProductInfo.class);
                    BigDecimal amount = calculateProductFee(productInfo, rateDetail.getActHandleReturnDetails(), rate, lastMileBaseRateInfo,billingItemList);
                    totalAmount = totalAmount.add(amount);
                }
            }
        }
        //计算折扣后金额
        BigDecimal discountedFee = RateCalculateUtils.calculateDiscountedFee(totalAmount, handleRateDTO.getDiscount());
        costCalculationFeeItemVO.setDiscount(handleRateDTO.getDiscount());
        costCalculationFeeItemVO.setFee(discountedFee);
        costCalculationFeeItemVO.setCurrency(handleCurrency);
        return costCalculationFeeItemVO;
    }

    private ValidationAddressDTO getValidationAddressDTO(PclOrderB2cSwitchChannelDTO pclOrderB2cSwitchChannelDTO) {
        ValidationAddressDTO validationAddressDTO=new ValidationAddressDTO();
        validationAddressDTO.setCountry(pclOrderB2cSwitchChannelDTO.getCountry());
        validationAddressDTO.setCity(pclOrderB2cSwitchChannelDTO.getCity());
        validationAddressDTO.setState(pclOrderB2cSwitchChannelDTO.getState());
        validationAddressDTO.setDistrict(pclOrderB2cSwitchChannelDTO.getDistrict());
        validationAddressDTO.setPostCode(pclOrderB2cSwitchChannelDTO.getPostCode());
        List<String>addressLines=new ArrayList<>();
        addressLines.add(pclOrderB2cSwitchChannelDTO.getAddressLine1());
        addressLines.add(pclOrderB2cSwitchChannelDTO.getAddressLine2());
        addressLines.add(pclOrderB2cSwitchChannelDTO.getAddressLine3());
        validationAddressDTO.setAddressLines(addressLines);
        return validationAddressDTO;
    }

    @Override
    public List<SubError> abandoned(List<String> orderIds) {
        List<SubError> errorList = new ArrayList<>();
        List<PclOrderB2c> pclOrderB2cList = pclOrderB2cDao.selectByIds(orderIds);
        List<PclOrderB2c> canAbandonedOrderList = new ArrayList<>();
        pclOrderB2cList.forEach(pclOrderB2c -> {
            if (!(B2cOrderStatus.UNCONFIRMED.getType().equals(pclOrderB2c.getStatus())
                    || B2cOrderStatus.SENDING_ABNORMALITY.getType().equals(pclOrderB2c.getStatus()))) {
                errorList.add(SubError.build(PclOrderB2cResultCode.ORDER_STATUS_CANNOT_ABANDONED));
            } else {
                canAbandonedOrderList.add(pclOrderB2c);
            }
        });
        if (Detect.notEmpty(errorList)) {
            return errorList;
        } else {
            //创建事件
            List<TrkSourceOrderEventDTO> events=new ArrayList<>();
            canAbandonedOrderList.forEach(canAbandonedOrder -> {
                canAbandonedOrder.setStatus(B2cOrderStatus.ABANDONED.getType());
                pclOrderB2cDao.updateByPrimaryKeySelective(canAbandonedOrder);
                orderEventService.packageEvents(canAbandonedOrder.getOrderNo(),canAbandonedOrder.getReferenceNo(),canAbandonedOrder.getId(),events, WareHouseOrderEventCode.ABANDONED);

            });
            orderEventService.addEventsByApi(events);

        }
        return errorList;
    }

    @Override
    public List<SubError> dispatchCheck(B2cOrderDispatchRequest req) {
        List<SubError> errors = Lists.newArrayList();
        if (!Detect.notEmpty(req.getItems())) {
            errors.add(SubError.build(PclOrderB2cResultCode.HANDLE_OPERATION_NOT_SUPPORT, req.getWarehouseCode()));
        } else {
            List<B2cOrderDispatchRequest.B2cOrderDispatchRequestItem> items = req.getItems();
            for (B2cOrderDispatchRequest.B2cOrderDispatchRequestItem item : items) {
                if (item.getProductQty() == null || item.getProductQty() <= 0) {
                    errors.add(SubError.build(PclOrderB2cResultCode.QTY_ERROR, item.getSku()));
                }
            }
        }
        //判读是否是未对接仓,只有未对接仓才可手动出库
        String warehouseCode = req.getWarehouseCode();
        Integer aggregator = SessionContext.getContext().getAggregator();

        SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(warehouseCode, aggregator);
        Integer platform = sysOverseasWarehouse.getPlatForm();
        if (!(platform == null || OverseasWarehousePlatform.getByCode(platform) == null)) {
            errors.add(SubError.build(PclOrderB2cResultCode.HANDLE_OPERATION_NOT_SUPPORT));
        }
        return errors;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public int dispatch(B2cOrderDispatchRequest req) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        int c = 0;
        Long orderId = req.getId();
        String trackingNo = req.getTrackingNo();
        final String warehouseCode = req.getWarehouseCode();
        Integer shipperId = req.getShipperId();
        if (shipperId == null || shipperId < 0) {
            shipperId = SessionContext.getContext().getTenantId();
        }
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();
        PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(orderId);;
        if (StringUtils.isNotEmpty(trackingNo)) {
            PclOrderB2c orderB2c = new PclOrderB2c();
            orderB2c.setId(orderId);
            orderB2c.setDateShipping(new Date());
            orderB2c.setStatus(B2cOrderStatus.OUTBOUND.getType());
            orderB2c.setTrackingNo(trackingNo);
            int i = pclOrderB2cDao.updateByPrimaryKeySelective(orderB2c);
            orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.OUT_OF_STORAGE);
            c += i;
        }
        final List<B2cOrderDispatchRequest.B2cOrderDispatchRequestItem> items = req.getItems();
        List<PclOrderB2cProductDTO> b2cProductList = Lists.newArrayList();
        for (B2cOrderDispatchRequest.B2cOrderDispatchRequestItem item : items) {
            final String sku = item.getSku();
            final Integer productQty = item.getProductQty();
            //更新装箱明细出库数量
            updateItems(item);
            //转换成通用出库商品实体 用于计算费用
            PclOrderB2cProductDTO b2cProductDTO = new PclOrderB2cProductDTO();
            b2cProductDTO.setSku(sku);
            b2cProductDTO.setQuantity(productQty);
            b2cProductDTO.setGrossWeightActual(item.getGrossWeightActual());
            b2cProductDTO.setProductHeightActual(item.getProductHeightActual());
            b2cProductDTO.setProductLengthActual(item.getProductLengthActual());
            b2cProductDTO.setProductWidthActual(item.getProductWidthActual());
            b2cProductList.add(b2cProductDTO);
            // 更新库龄数据
            removeAgingCount(sku, warehouseCode, shipperId, productQty,pclOrderB2c);
            // 更新待出库商品数量  待出库的商品减少了 productQty 个
            PclInventory reqCondition = new PclInventory();
            reqCondition.setSku(sku);
            reqCondition.setOverseasWarehouseCode(warehouseCode);
            reqCondition.setAggregator(SessionContext.getContext().getAggregator());
            reqCondition.setShipperId(shipperId);
            final PclInventory inventory = pclInventoryDao.selectLimitOne(reqCondition);
            if (inventory == null) {
                log.error("not find the inventory record with sku:{} and overseas warehouse:{}", sku, warehouseCode);
            } else {
                PclInventory waitForUpdate = new PclInventory();
                int count = inventory.getOutgoingInventory() == null ? 0 : inventory.getOutgoingInventory();
                PclBatch batchForForecastCount = getBatchBySkuAndOrderAndWareshouse(orderId, sku, warehouseCode);
                int forecastCount = batchForForecastCount == null ? 0 : batchForForecastCount.getForecastAmount();
                final int outGoingCount = count - forecastCount;
                waitForUpdate.setOutgoingInventory(outGoingCount);
                log.info("{} 更新待出库数量: {} - {} = {}", inventory.getId(), count, forecastCount, outGoingCount);
                int availableCount = inventory.getAvailableInventory() == null ? 0 : inventory.getAvailableInventory();
                final int availableCountAfterCal = availableCount - (productQty - forecastCount);
                waitForUpdate.setAvailableInventory(availableCountAfterCal);
                log.info("{} 更新可用数量: {} - ({} - {}) = {}", inventory.getId(), availableCount, productQty,
                        forecastCount, availableCountAfterCal);
                waitForUpdate.setId(inventory.getId());
                int i = pclInventoryDao.outgoingInventoryCount(waitForUpdate, forecastCount - productQty, -1 * forecastCount);
                c += i;
            }
            // 更新 批次状态 和 变更的数量
            PclBatch pclBatch = new PclBatch();
            pclBatch.setOrderId(orderId);
            pclBatch.setStatus(BatchStatus.OUTBOUND.getStatus());
            pclBatch.setWarehousingAmount(productQty);
            Example example = getSearchConditionForBatchData(orderId, sku, warehouseCode);
            final int i = pclBatchDao.updateByExampleSelective(pclBatch, example);
            if (i == 0) {
                log.error("not update batch for sku:{} and overseas warehouse code is: {} orderId:{}", sku, warehouseCode, orderId);
            } else {
                c += i;
            }
        }
        //生成派送费
        PclOrderB2cOrderDTO pclOrderB2cOrderDTO = new PclOrderB2cOrderDTO();
        pclOrderB2cOrderDTO.setProducts(b2cProductList);
        //特殊需求 要求写死 宁兴优贝 集成商 使用运费测算接口获取运费 16028正式环境id 15355 qa环境id
//        if(Objects.equals(aggregator, 16028) || Objects.equals(aggregator, 15355) ){
//            //需要进行接口调用获取派送费
//            calculateLastMileFeeByRateInquiry(pclOrderB2c,b2cProductList);
//        }else{
//        }
        //出库生成费用
        triggerOutboundFee(pclOrderB2c, pclOrderB2cOrderDTO);
        orderEventService.addEventsByApi(events);
        return c;
    }

    @Override
    public void calculateLastMileFeeByRateInquiry(PclOrderB2c pclOrderB2c,List<PclOrderB2cProductDTO> b2cProductList) {
        Integer shipperId = pclOrderB2c.getShipperId();
        String shipperWarehouseCode = pclOrderB2c.getShipperWarehouseCode();
        SysOverseasWarehouseChannelDTO sysOverseasWarehouseChannelDTO = sysOverseasWarehouseChannelDao.selectWarehouseChannelDetailById(pclOrderB2c.getChannelId());
        //需要进行接口调用获取派送费
        SysParty shipper = sysPartyService.getByPartyId(shipperId);
        B2cRateInquiryRequestDTO b2cRateInquiryRequestDTO=convertB2cRateInquiryRequestDTOForOutbound(pclOrderB2c,b2cProductList,sysOverseasWarehouseChannelDTO);
        //调用费用测算接口
        B2cRateInquiryResponseDTO b2cRateInquiryResponseDTO = b2cShipperService.rateInquiry(b2cRateInquiryRequestDTO, shipper);
        if(!Detect.notEmpty(b2cRateInquiryResponseDTO.getErrorMessage())){
            //查询海外仓尾程价卡配置信息
            String dateStr = DateUtils.format(new Date(), DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1);
            List<SysOverseasWarehouseShipperRateDetailDTO> detailDTOS = sysOverseasWarehouseShipperDao.getLastMileRateByShipperAndCode(pclOrderB2c.getShipperId(), shipperWarehouseCode, pclOrderB2c.getChannelId(), dateStr);
            if (!Detect.notEmpty(detailDTOS)) {
                log.info("集成商:{},发货人:{},海外仓:{},服务:{},尾程价卡配置为空", pclOrderB2c.getAggregator(), pclOrderB2c.getShipperId(), shipperWarehouseCode, pclOrderB2c.getServiceCode());
            }
            Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap = detailDTOS.stream().collect(Collectors.groupingBy(p -> p.getRateType()));
            //尾程-基础运费-价卡配置
            List<SysOverseasWarehouseShipperRateDetailDTO> lastMileBaseFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getType().shortValue());
            SysOverseasWarehouseShipperRateDetailDTO lastMileRateConfig = Detect.notEmpty(lastMileBaseFeeRateList) ? lastMileBaseFeeRateList.get(0) : null;
            //尾程-附加费-价卡配置
            List<SysOverseasWarehouseShipperRateDetailDTO> lastMileSurchargeFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.LAST_MILE_SURCHARGE_FEE.getType().shortValue());
            SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig = Detect.notEmpty(lastMileSurchargeFeeRateList) ? lastMileSurchargeFeeRateList.get(0) : null;

            ArrayList<ActOverseasWarehouseBillItemDTO> billItems = Lists.newArrayList();
            //计费重
            BigDecimal weight = b2cRateInquiryResponseDTO.getWeight();
            BigDecimal totalAmount = b2cRateInquiryResponseDTO.getTotalAmount();
            //币种
            String currency = b2cRateInquiryResponseDTO.getCurrency();

            //基础运费 * etowerOne折扣
            BigDecimal amount = b2cRateInquiryResponseDTO.getAmount();
            Integer lastMileRateDiscount =lastMileRateConfig != null ? lastMileRateConfig.getDiscount() : 0;
            BigDecimal discountedAmount = RateCalculateUtils.calculateDiscountedFee(amount, lastMileRateDiscount);
            if(amount.compareTo(BigDecimal.ZERO)!=0){
                ActOverseasWarehouseBillItemDTO baseFeeItem = getActOverseasWarehouseBillItemByType(amount,weight,currency,LastMileFeeType.LAST_MILE_BASE_FEE.getCode(),LastMileFeeType.LAST_MILE_BASE_FEE.getCode(),null,null);
                baseFeeItem.setDiscount(lastMileRateDiscount);
                baseFeeItem.setDiscountedAmount(discountedAmount);
                billItems.add(baseFeeItem);
            }
            //关税
            BigDecimal tax = b2cRateInquiryResponseDTO.getTax();
            if(tax.compareTo(BigDecimal.ZERO)!=0){
                ActOverseasWarehouseBillItemDTO taxItem =getActOverseasWarehouseBillItemByType(tax,weight,currency,LastMileFeeType.LAST_MILE_TAX_FEE.getCode(),LastMileTaxType.TARIFF_TAX_RATE.getCode(),null,null);
                billItems.add(taxItem);
            }
            //增值税
            BigDecimal vat = b2cRateInquiryResponseDTO.getVat();
            if(vat.compareTo(BigDecimal.ZERO)!=0){
                ActOverseasWarehouseBillItemDTO vatItem =getActOverseasWarehouseBillItemByType(vat,weight,currency,LastMileFeeType.LAST_MILE_TAX_FEE.getCode(),LastMileTaxType.LAST_MILE_VAT_RATE.getCode(),null,null);
                billItems.add(vatItem);
            }
            //其他附加费
            BigDecimal surcharge = b2cRateInquiryResponseDTO.getSurcharge();
            if(surcharge.compareTo(BigDecimal.ZERO)!=0){
                ActOverseasWarehouseBillItemDTO surchargeItem =getActOverseasWarehouseBillItemByType(surcharge,weight,currency,LastMileFeeType.LAST_MILE_SURCHARGE_FEE.getCode(),LastMileSurchargeType.LAST_MILE_OTHER_SURCHARGE.getCode(),null,null);
                billItems.add(surchargeItem);
            }
            //电池附加费
            BigDecimal batteryCharge = b2cRateInquiryResponseDTO.getBatteryCharge();
            if(batteryCharge.compareTo(BigDecimal.ZERO)!=0){
                ActOverseasWarehouseBillItemDTO batteryChargeItem =getActOverseasWarehouseBillItemByType(batteryCharge,weight,currency,LastMileFeeType.LAST_MILE_SURCHARGE_FEE.getCode(),LastMileSurchargeType.BATTERY_SURCHARGE.getCode(),null,null);
                billItems.add(batteryChargeItem);
            }
//            //燃油附加费 不从小包获取 由etowerOne百分比燃油价卡计算
//            BigDecimal fuelSurcharge = b2cRateInquiryResponseDTO.getFuelSurcharge();
//            if(fuelSurcharge.compareTo(BigDecimal.ZERO)!=0){
//                ActOverseasWarehouseBillItemDTO fuelSurchargeItem =getActOverseasWarehouseBillItemByType(fuelSurcharge,weight,currency,LastMileFeeType.LAST_MILE_SURCHARGE_FEE.getCode(),LastMileSurchargeType.LAST_MILE_FUEL_SURCHARGE.getCode(),null,null);
//                billItems.add(fuelSurchargeItem);
//            }
            //偏远地区附加费
            BigDecimal remoteAreaSurcharge = b2cRateInquiryResponseDTO.getRemoteAreaSurcharge();
            if(remoteAreaSurcharge.compareTo(BigDecimal.ZERO)!=0){
                ActOverseasWarehouseBillItemDTO remoteAreaSurchargeItem =getActOverseasWarehouseBillItemByType(remoteAreaSurcharge,weight,currency,LastMileFeeType.LAST_MILE_SURCHARGE_FEE.getCode(),LastMileSurchargeType.LAST_MILE_REMOTE_AREA_SURCHARGE.getCode(),null,null);
                billItems.add(remoteAreaSurchargeItem);
            }
            //特殊附加费
            List<B2cRateInquiryForSpecialSurchargeDTO> specialSurchargeList = b2cRateInquiryResponseDTO.getBillDetailSpecialDTOList();
            if(Detect.notEmpty(specialSurchargeList)){
                specialSurchargeList.forEach(specialSurcharge->{
                    BigDecimal specialSurchargeAmount = specialSurcharge.getAmount();
                    if(specialSurchargeAmount.compareTo(BigDecimal.ZERO)!=0){
                        String feeNameCn = specialSurcharge.getFeeNameCn();
                        String feeName = specialSurcharge.getFeeName();
                        ActOverseasWarehouseBillItemDTO specialSurchargeItem =getActOverseasWarehouseBillItemByType(specialSurchargeAmount,weight,currency,LastMileFeeType.LAST_MILE_SURCHARGE_FEE.getCode(),LastMileSurchargeType.SPECIAL_SURCHARGE.getCode(),feeNameCn,feeName);
                        billItems.add(specialSurchargeItem);
                    }
                });
            }
            //小包总费用 - 基本运费折扣
            totalAmount = BigDecimalUtils.covertNullToZero(totalAmount)
                    .subtract(BigDecimalUtils.covertNullToZero(b2cRateInquiryResponseDTO.getAmount()))
                    .add(BigDecimalUtils.covertNullToZero(discountedAmount)) ;

            //燃油附加费 （使用etowerOne价卡计算 与客户约定不在小包配置燃油价卡）
            BigDecimal fuelFee = calculateFuelFee(surchargeRateConfig, totalAmount, currency, billItems);
            //总费用 = 小包总费用 - 基本运费折扣 + 燃油附加费
            totalAmount = totalAmount.add(BigDecimalUtils.covertNullToZero(fuelFee));
            //扣减费用
            ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
            create.setAmount(totalAmount);
            //保持一致 存基本运费的折扣
            create.setDiscount(lastMileRateConfig.getDiscount());
            create.setCurrency(currency);
            create.setBillItems(billItems);
            lastMileCalculateFeeService.deductionFee(pclOrderB2c, create);
        }
    }

    /**
     * 宁心优贝定制临时方案-计算燃油附加费
     * @param surchargeRateConfig
     * @param totalFee
     * @param currency
     * @param billItems
     * @return
     */
    private BigDecimal calculateFuelFee(SysOverseasWarehouseShipperRateDetailDTO surchargeRateConfig, BigDecimal totalFee, String currency, List<ActOverseasWarehouseBillItemDTO> billItems) {
        if (surchargeRateConfig == null || surchargeRateConfig.getRateId() == null) {
            return BigDecimal.ZERO;
        }
        //查询附加费方案
        Long surchargeConfigId = surchargeRateConfig.getRateId();
        ActLastMileSurchargeConfig surchargeConfig = lastMileSurchargeConfigDao.selectByPrimaryKey(surchargeConfigId);
        if (!Objects.equals(surchargeConfig.getActive(), ActiveTypeNew.ACTIVE.getType())) {
            log.info("附加费方案:{}已经失效", surchargeConfigId);
            return BigDecimalUtils.ZERO;
        }
        log.info("附加费方案名称:{}, 附加费ID:{}", surchargeConfig.getName(), surchargeConfigId);
        //查询附加费方案下配置的附加费
        List<ActLastMileSurchargeConfigItem> configItems = lastMileSurchargeConfigItemDao.getByConfigId(surchargeConfigId);
        if (!Detect.notEmpty(configItems)) {
            log.info("附加费方案ID:{}未配置任何的附加费价卡", surchargeConfigId);
            return BigDecimalUtils.ZERO;
        }
        //筛选出燃油附加费价卡
        ActLastMileSurchargeConfigItem fuelSurchargeConfigItem = configItems.stream()
                .filter(p -> Objects.equals(p.getSurchargeType(), LastMileSurchargeType.LAST_MILE_FUEL_SURCHARGE.getCode())).findFirst().orElse(null);
        if (fuelSurchargeConfigItem == null) {
            log.info("附加费方案ID:{}未配置燃油附加费价卡", surchargeConfigId);
            return BigDecimal.ZERO;
        }
        //查询燃油附加费价卡
        List<ActLastMileSurcharge> fuelSurchargeRates = actLastMileSurchargeDao.selectByIds(String.valueOf(fuelSurchargeConfigItem.getSurchargeId()));
        if (Detect.empty(fuelSurchargeRates)) {
            return BigDecimal.ZERO;
        }
        //判断价卡是否为生效状态
        ActLastMileSurcharge fuelSurchargeRate = fuelSurchargeRates.get(0);
        Long fuelSurchargeId = fuelSurchargeRate.getId();
        if (!Objects.equals(fuelSurchargeRate.getActive(), ActiveTypeNew.ACTIVE.getType())) {
            log.info("燃油附加费价卡名称：{}, ID:{}为失效状态，无法使用", fuelSurchargeRate.getName(), fuelSurchargeId);
            return BigDecimal.ZERO;
        }
        //计算费用
        BigDecimal fuelFee = getFuelFee(fuelSurchargeId, totalFee);
        //币种转换 将价卡币种转换为小包获取费用的币种
        final BigDecimal convertFuelFee = sysChinaBankExchangeRateService.getProductInvoiceValue(fuelFee,
                surchargeConfig.getCurrency(), currency, new HashMap<>());
        //费用明细
        ActOverseasWarehouseBillItemDTO billItemDTO = new ActOverseasWarehouseBillItemDTO();
        billItemDTO.setCostType(LastMileFeeType.LAST_MILE_SURCHARGE_FEE.getCode());
        billItemDTO.setCostSubType(LastMileSurchargeType.LAST_MILE_FUEL_SURCHARGE.getCode());
        billItemDTO.setAmount(BigDecimalUtils.covertNullToZero(convertFuelFee).setScale(3, RoundingMode.HALF_UP));
        billItemDTO.setCurrency(currency);
        billItemDTO.setDiscount(0);
        billItemDTO.setDiscountedAmount(convertFuelFee);
        billItems.add(billItemDTO);
        return convertFuelFee;
    }

    /**
     * 宁心优贝临时定制方案-etowerOne计算燃油附加费
     * 小包获取到的费用在经过etowerOne基本运费折扣后汇总金额 作为基础金额通过百分比方式计算得到燃油附加费
     * @param fuelSurchargeId
     * @param lastMileFee
     * @return
     */
    public BigDecimal getFuelFee(Long fuelSurchargeId, BigDecimal lastMileFee) {
        log.info("计算燃油附加费Start");
        if (fuelSurchargeId == null) {
            log.info("当前附加费未配置燃油附加费,无法计算燃油附加费");
            return BigDecimal.ZERO;
        }
        List<ActLastMileSurchargeFuelItemDTO> surchargeFuelItemDTOList = lastMileSurchargeFuelItemDao.selectBySurchargeId(fuelSurchargeId);
        if (!Detect.notEmpty(surchargeFuelItemDTOList)) {
            log.info("当前燃油附加费条目详情为空,无法计算燃油附加费");
            return BigDecimal.ZERO;
        }
        RateCheckUtils.ValidityDateVo validDateItem = RateCheckUtils.getValidDateItem(surchargeFuelItemDTOList, new Date());
        if (validDateItem == null) {
            log.info("燃油附加费ID:{}, 条目详情 没有在有效期内的item", fuelSurchargeId);
            return BigDecimal.ZERO;
        }
        ActLastMileSurchargeFuelItemDTO surchargeFuelItemDTO = surchargeFuelItemDTOList.stream()
                .filter(item -> item.getId().equals(validDateItem.getId()))
                .collect(Collectors.toList()).get(0);
        Short unit = surchargeFuelItemDTO.getUnit();
        BigDecimal surchargeFee = surchargeFuelItemDTO.getSurchargeFee();
        LastMileSurchargeBillingUnitType unitType = LastMileSurchargeBillingUnitType.getByCode(unit);
        //燃油附加费
        BigDecimal fuelFee = BigDecimal.ZERO;
        //只计算百分比类型
        if (unitType != null && Objects.equals(unitType, LastMileSurchargeBillingUnitType.PERCENT)) {
            if (surchargeFee != null) {
                BigDecimal percent = surchargeFee.divide(new BigDecimal(100), 4, BigDecimal.ROUND_HALF_UP);
                fuelFee = BigDecimalUtils.covertNullToZero(lastMileFee).multiply(percent);
            }
        } else {
            log.info("命中有效期区间的计费类型不是百分比，将不计费");
            return BigDecimal.ZERO;
        }
        log.info("燃油附加费:{},计算燃油附加费End", fuelFee);
        return fuelFee;
    }

    @Override
    public List<PclOrderB2c> getNeedRateInquiryOrderList(Integer aggregator) {
        //默认查今天之前60天数据
        Date date=new Date();
        Date dateBefore = DateUtils.getDateBefore(date, 60);
        return pclOrderB2cDao.getNeedRateInquiryOrderList(aggregator,dateBefore,date);
    }

    @Override
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRED)
    public List<SubError> confirm(PclOrderB2cConfirmRequest pclOrderB2cConfirmRequest) {
        List<SubError> errors = new ArrayList<>();
        PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(pclOrderB2cConfirmRequest.getOrderId());
        if (pclOrderB2c == null) {
            errors.add(SubError.build(PclOrderTransportResultCode.ORDER_NOT_FIND));
            return errors;
        }
        Short status = pclOrderB2c.getStatus();
        if (!status.equals(B2cOrderStatus.OUTBOUND.getType())) {
            errors.add(SubError.build(PclOrderTransportResultCode.CONFIRM_STATUS_ERROR));
            return errors;
        }
        pclOrderB2c.setTransportType(pclOrderB2cConfirmRequest.getTransportType());
        //更新订单物流信息，生成出库杂费费用
        doConfirmOrderInfo(pclOrderB2c,pclOrderB2cConfirmRequest);
        return errors;
    }

    private void doConfirmOrderInfo(PclOrderB2c pclOrderB2c, PclOrderB2cConfirmRequest pclOrderB2cConfirmRequest) {
        Integer standardPalletCount = pclOrderB2cConfirmRequest.getStandardPalletCount();
        Integer nonStandardPalletCount = pclOrderB2cConfirmRequest.getNonStandardPalletCount();
        Integer labelCount = pclOrderB2cConfirmRequest.getLabelCount();
        Integer trunkCount = pclOrderB2cConfirmRequest.getTrunkCount();
        BigDecimal packingSpecification = pclOrderB2cConfirmRequest.getPackingSpecification();
        if (packingSpecification != null) {
            pclOrderB2c.setPackingSpecification(packingSpecification);
        }
        if (pclOrderB2c.getTransportType()!=null&&pclOrderB2c.getTransportType().equals(OrderTransportType.TRUCK.getType())) {
            if (standardPalletCount != null) {
                pclOrderB2c.setStandardPalletCount(standardPalletCount);
            }
            if (nonStandardPalletCount != null) {
                pclOrderB2c.setNonStandardPalletCount(nonStandardPalletCount);
            }
            if (labelCount != null) {
                pclOrderB2c.setLabelCount(labelCount);
            }
            if (trunkCount != null) {
                pclOrderB2c.setTrunkCount(trunkCount);
            }
        }
        if (pclOrderB2c.getTransportType()!=null&&pclOrderB2c.getTransportType().equals(OrderTransportType.EXPRESS.getType())) {
            List<String> trackingNoList = pclOrderB2cConfirmRequest.getTrackingNoList();
            if(Detect.notEmpty(trackingNoList)&&!Detect.notEmpty(pclOrderB2c.getTrackingNo())){
                String trackingNo = org.apache.commons.lang.StringUtils.join(trackingNoList, Constants.SplitConstant.SEMICOLON);
                pclOrderB2c.setTrackingNo(trackingNo);
            }
        }
        //设置提交时间
        pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
        //计算出库杂费
        calculateOutboundMiscellaneousFee(pclOrderB2c);
    }

    /**
     * 计算出库杂费
     * 包含：拣货费、包装费、订单处理费、打托费、自提附加费
     * @param pclOrderB2c
     */
    private void calculateOutboundMiscellaneousFee(PclOrderB2c pclOrderB2c) {
        Integer shipperId = pclOrderB2c.getShipperId();
        String shipperWarehouseCode = pclOrderB2c.getShipperWarehouseCode();
        Long channelId = pclOrderB2c.getChannelId();
        Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap = sysOverseasWarehouseShipperService.getOutboundMiscellaneousRateConfigInfo(shipperId, shipperWarehouseCode, channelId);
        if (Detect.notEmpty(rateDetailMap)) {
            //获取商品信息
            List<B2cProductInfo>productInfoList=pclProductB2cDao.getProductSkuCountInfo(Collections.singletonList(pclOrderB2c.getOrderNo()));
            createOutboundMiscellaneousFeeForConfirm(pclOrderB2c,rateDetailMap,productInfoList);
        }
    }

    @Override
    public void createOutboundMiscellaneousFeeForOutbound(PclOrderB2c pclOrderB2c, Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap, List<B2cProductInfo> productInfoList) {
        //出库杂费：拣货费
        List<SysOverseasWarehouseShipperRateDetailDTO> pickingFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.PICKING_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO pickingFeeConfig = Detect.notEmpty(pickingFeeRateList) ? pickingFeeRateList.get(0) : null;
        if(pickingFeeConfig!=null){
            createPickingFee(pickingFeeConfig,productInfoList,pclOrderB2c);
        }
        //出库杂费：订单处理费
        List<SysOverseasWarehouseShipperRateDetailDTO> orderHandlingFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.ORDER_HANDLING_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO orderHandlingFeeConfig = Detect.notEmpty(orderHandlingFeeRateList) ? orderHandlingFeeRateList.get(0) : null;
        if(orderHandlingFeeConfig!=null){
            createOrderHandlingFee(orderHandlingFeeConfig,pclOrderB2c);
        }
        //出库杂费：自提附加费
        List<SysOverseasWarehouseShipperRateDetailDTO> pickUpFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.PICK_UP_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO pickUpFeeRateConfig = Detect.notEmpty(pickUpFeeRateList) ? pickUpFeeRateList.get(0) : null;
        if(pickUpFeeRateConfig!=null && Objects.equals(pickUpFeeRateConfig.getBillingType(), OverseasWarehouseRateBillingType.ORDER.getType())){
            createPickUpFee(pickUpFeeRateConfig,pclOrderB2c);
        }
    }

    @Override
    public void createOutboundMiscellaneousFeeForConfirm(PclOrderB2c pclOrderB2c, Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap, List<B2cProductInfo> productInfoList) {
        //出库杂费：包装费
        List<SysOverseasWarehouseShipperRateDetailDTO> packingFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.PACKING_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO packingFeeRateConfig = Detect.notEmpty(packingFeeRateList) ? packingFeeRateList.get(0) : null;
        if(packingFeeRateConfig!=null){
            createPackingFee(packingFeeRateConfig,pclOrderB2c);
        }
        //出库杂费：打托费
        List<SysOverseasWarehouseShipperRateDetailDTO> palletFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.PALLET_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO palletFeeRateConfig = Detect.notEmpty(palletFeeRateList) ? palletFeeRateList.get(0) : null;
        if(palletFeeRateConfig!=null){
            createPalletFee(palletFeeRateConfig,pclOrderB2c);
        }
        //出库杂费：自提附加费
        List<SysOverseasWarehouseShipperRateDetailDTO> pickUpFeeRateList = rateDetailMap.get(OverseasWarehouseRateType.PICK_UP_FEE.getType().shortValue());
        SysOverseasWarehouseShipperRateDetailDTO pickUpFeeRateConfig = Detect.notEmpty(pickUpFeeRateList) ? pickUpFeeRateList.get(0) : null;
        if(pickUpFeeRateConfig!=null && !Objects.equals(pickUpFeeRateConfig.getBillingType(), OverseasWarehouseRateBillingType.ORDER.getType())){
            createPickUpFee(pickUpFeeRateConfig,pclOrderB2c);
        }
    }

    @Override
    public List<ActOverseasWarehouseBillMergeDTO> billDetail(Long orderId) {
        PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(orderId);
        if (pclOrderB2c == null) {
            return null;
        }
        List<ActOverseasWarehouseBillMergeDTO> billMergeDTOs = actOverseasWarehouseBillService.getBillItemByOrderNoOrTrackingNo(pclOrderB2c.getOrderNo(), pclOrderB2c.getTrackingNo());
        return billMergeDTOs;
    }

    @Override
    public List<PclOrderReturnDTO> getRelatedReturnOrderList(Long id) {
        PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(id);
        List<PclOrderReturnDTO>pclOrderReturnDTOList=new ArrayList<>();
        if(pclOrderB2c!=null){
            pclOrderReturnDTOList= pclOrderReturnDao.selectByB2cOrderNo(pclOrderB2c.getOrderNo());
            pclOrderReturnDTOList.forEach(pclOrderReturnDTO -> {
                Short status = pclOrderReturnDTO.getStatus();
                if (status != null) {
                    OrderReturnStatus orderReturnStatus = OrderReturnStatus.get(status);
                    if (orderReturnStatus != null) {
                        pclOrderReturnDTO.setStatusMsg(orderReturnStatus.getMessage());
                    }
                }
                Short type = pclOrderReturnDTO.getType();
                if (type != null) {
                    OrderReturnType orderReturnType = OrderReturnType.getType(type);
                    if (orderReturnType != null) {
                        pclOrderReturnDTO.setTypeMsg(orderReturnType.getMessage());
                    }
                }
                //获取费用
                List<ActOverseasWarehouseBillMergeDTO> billByOrderNoList = actOverseasWarehouseBillService.getBillByOrderNo(pclOrderReturnDTO.getOrderNo());
                //没有扣费金额和币种的不计算总值
                List<ActOverseasWarehouseBillMergeDTO> nonConvertAmountBillList = billByOrderNoList.stream().filter(bill -> !Detect.notEmpty(bill.getConvertCurrency()) || bill.getConvertAmount() == null).collect(Collectors.toList());
                if(!Detect.notEmpty(nonConvertAmountBillList)){
                    List<ActOverseasWarehouseBillMergeDTO>billListGroupByCurrency=new ArrayList<>();
                    billByOrderNoList.stream().collect(Collectors.groupingBy(ActOverseasWarehouseBillMergeDTO::getConvertCurrency)).forEach((currency,billList)->{
                        ActOverseasWarehouseBillMergeDTO actOverseasWarehouseBillMergeDTO=new ActOverseasWarehouseBillMergeDTO();
                        BigDecimal totalAmount = billList.stream().map(ActOverseasWarehouseBillMergeDTO::getConvertAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
                        actOverseasWarehouseBillMergeDTO.setConvertCurrency(currency);
                        actOverseasWarehouseBillMergeDTO.setConvertAmount(totalAmount);
                        billListGroupByCurrency.add(actOverseasWarehouseBillMergeDTO);
                    });
                    pclOrderReturnDTO.setBillListGroupByCurrency(billListGroupByCurrency);
                }
            });
        }
        return pclOrderReturnDTOList;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public List<SubError> forceDestroy(List<Long> ids) {
        List<SubError> errors = Lists.newArrayList();
        log.info("更新订单...");
        List<PclOrderB2c> pclOrderB2cList = forceDestroyOrder(ids, errors);
        if (Detect.notEmpty(errors)) {
            return errors;
        }
        log.info("通知小包自提超时销毁成功...");
        pclOrderB2cList.forEach(pclOrderB2c -> {
            notifyB2cCainiao(pclOrderB2c);
        });
        return errors;
    }

    @Override
    public void handleBatchAndInventory(PclOrderB2c pclOrderB2c) {
        Long orderId = pclOrderB2c.getId();
        //订单商品列表纵转横（使用商品列表更新库存和批次）
        HashMap<String, PclProductB2cGoodDefectiveDTO> skuMap = buildGoodDefectiveProduct(orderId);
        //>>>>>1.扣减库龄 获取订单关联的 库龄出库记录
        List<PclInventoryLog> pclInventoryLogList = pclInventoryLogDao.getByOrderId(orderId);
        if (Detect.notEmpty(pclInventoryLogList)) {
            List<PclInventoryAging> inventoryAgingList = Lists.newArrayList();
            pclInventoryLogList.forEach(pclInventoryLog -> {
                Long inventoryAgingId = pclInventoryLog.getInventoryAgingId();
                Integer changeCount = pclInventoryLog.getChangeCount();
                Short inventoryType = pclInventoryLog.getInventoryType();
                PclInventoryAging pclInventoryAging = pclInventoryAgingDao.selectByPrimaryKey(inventoryAgingId);
                if (pclInventoryAging != null) {
                    PclInventoryAging updateAging = new PclInventoryAging();
                    updateAging.setId(pclInventoryAging.getId());
                    if (Objects.equals(inventoryType, ProductInventoryType.DEFECTIVE_PRODUCT.getCode())) {
                        //不良品
                        updateAging.setDefectiveCurrentCount(pclInventoryAging.getDefectiveCurrentCount() - changeCount);
                    } else {
                        //良品
                        updateAging.setCurrentCount(pclInventoryAging.getCurrentCount() - changeCount);
                    }
                    updateAging.initUpdateEntity();
                    pclInventoryAgingDao.updateByPrimaryKeySelective(updateAging);
                    inventoryAgingList.add(pclInventoryAging);
                }
            });
            //记录代发订单和入库单的关联关系
            orderPoRelationService.recordOrderB2cRelation(pclOrderB2c, inventoryAgingList);

        } else {
            XxlJobLogger.log("orderId:" + orderId + "未找到InventoryLog数据");
        }
        //>>>>>扣减库存 批次
        List<PclBatch> pclBatchList = pclBatchDao.queryByOrderId(orderId);
        if (Detect.notEmpty(pclBatchList)) {
            //更新待出库
            pclBatchList.stream().collect(Collectors.groupingBy(PclBatch::getSku)).forEach((sku, skuList) -> {
                PclProductB2cGoodDefectiveDTO goodDefectiveDTO = skuMap.get(sku);
                if (goodDefectiveDTO == null) {
                    XxlJobLogger.log("订单的商品列表sku:{}不存在", sku);
                    return;
                }
                PclInventory pclInventory = pclInventoryDao.findBySkuAndWarehouseCode(sku, pclOrderB2c.getShipperWarehouseCode(), pclOrderB2c.getShipperId());
                //良品 + 不良品 的出库数量
                Integer goodProductCount = Optional.ofNullable(goodDefectiveDTO.getGoodProductCount()).orElse(0);
                Integer defectiveProductCount = Optional.ofNullable(goodDefectiveDTO.getDefectiveProductCount()).orElse(0);
                Integer totalOutgoingQty = goodProductCount + defectiveProductCount;
                pclInventoryDao.outgoingInventoryCount(pclInventory, 0, -1 * (totalOutgoingQty));
                skuList.forEach(pclBatch -> {
                    pclBatch.initUpdateEntity();
                    pclBatch.setStatus(BatchStatus.OUTBOUND.getStatus());
                    pclBatch.setWarehousingAmount(goodProductCount);
                    pclBatch.setDefectiveWarehousingAmount(defectiveProductCount);
                    pclBatchDao.updateByPrimaryKeySelective(pclBatch);
                });
            });
        }
    }

    @Override
    public void printPickingList(PclOrderB2cPrintPickingListRequest request, HttpServletResponse response) {
        List<PclOrderB2c> pclOrderB2cList = pclOrderB2cDao.selectByIdList(request.getOrderIds());
        //倒序
        pclOrderB2cList = pclOrderB2cList.stream().sorted(Comparator.comparing(PclOrderB2c::getDateCreated).reversed()).collect(toList());
        List<PclOrderB2cPickingListDTO> dataList = Lists.newArrayList();
        pclOrderB2cList.stream().forEach(pclOrderB2c -> {
            PclOrderB2cPickingListDTO pickingListDTO = getOrderPickingList(pclOrderB2c, response);
            dataList.add(pickingListDTO);
        });
        try {
            templatePdfExporter.export("b2c_order_picking_list_A4.jasper", response.getOutputStream(), dataList, new HashMap<>());
        } catch (Exception e) {
            throw new RuntimeException("export picking list failed:" + e.getMessage(), e);
        }
    }

    @Override
    public List<SubError> triggerOutboundFee(PclOrderB2c pclOrderB2c, PclOrderB2cOrderDTO pclOrderB2cOrderDTO) {
        //生成派送费用
        lastMileCalculateFeeService.calculateLastMileFee(pclOrderB2c, pclOrderB2cOrderDTO, false);
        //生成出库杂费
        Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap = sysOverseasWarehouseShipperService.getOutboundMiscellaneousRateConfigInfo(pclOrderB2c.getShipperId(), pclOrderB2c.getShipperWarehouseCode(), pclOrderB2c.getChannelId());
        if(Detect.notEmpty(rateDetailMap)){
            List<B2cProductInfo>productInfoList=pclProductB2cDao.getProductSkuCountInfo(Collections.singletonList(pclOrderB2c.getOrderNo()));
            createOutboundMiscellaneousFeeForOutbound(pclOrderB2c,rateDetailMap,productInfoList);
        }
        //生成自定义费用
        lastMileCalculateFeeService.calculateCustomFee(pclOrderB2c, pclOrderB2cOrderDTO);
        return null;
    }

    private PclOrderB2cPickingListDTO getOrderPickingList(PclOrderB2c pclOrderB2c, HttpServletResponse response) {
        //订单打印信息
        String now = DateUtils.format(new Date(), DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1);
        PclOrderB2cPickingListDTO dataObject = new PclOrderB2cPickingListDTO();
        dataObject.setOrderNo(pclOrderB2c.getOrderNo());
        dataObject.setCreateDate(now);
        List<PclOrderB2cDetailItemVO> productB2cList = pclProductB2cDao.getByOrderId(pclOrderB2c.getId());
        //查询可用库龄库位
        List<String> thirdSkuNoList = productB2cList.stream().map(p -> p.getThirdSkuNo()).collect(toList());
        WmsInventoryAgingQueryRequest agingQueryRequest = new WmsInventoryAgingQueryRequest();
        agingQueryRequest.setSkuList(thirdSkuNoList);
        agingQueryRequest.setWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
        Result<List<WmsStorageAgingDTO>> result = outboundOrderServiceFeign.getAvailableInventoryAgingList(agingQueryRequest);
        List<WmsStorageAgingDTO> availableInventoryAgingList = result.getData();
        Map<String, String> skuRecommendLocationCodeMap = availableInventoryAgingList.stream()
                .collect(Collectors.groupingBy(p -> p.getSku(), Collectors.collectingAndThen(toList(), o -> o.get(0).getLocationName())));
        //遍历商品 组装数据
        List<PclOrderB2cPickingListDTO.ProductInfo> productInfoList = productB2cList.stream().map(product -> {
            PclOrderB2cPickingListDTO.ProductInfo productInfo = new PclOrderB2cPickingListDTO.ProductInfo();
            productInfo.setSku(product.getSku());
            productInfo.setThirdSkuNo(product.getThirdSkuNo());
            productInfo.setSkuNameCn(product.getSkuNameCn());
            productInfo.setSkuNameEn(product.getSkuNameEn());
            productInfo.setProductQty(Optional.ofNullable(product.getProductCount()).orElse(0L).intValue());
            //推荐库位
            productInfo.setRecommendLocation(skuRecommendLocationCodeMap.get(product.getThirdSkuNo()));
            return productInfo;
        }).collect(toList());
        dataObject.setProductItems(new JRBeanCollectionDataSource(productInfoList));
        return dataObject;
    }

    /**
     * 将商品列表纵转横
     * @param orderId
     * @return
     */
    private HashMap<String, PclProductB2cGoodDefectiveDTO> buildGoodDefectiveProduct(Long orderId) {
        List<PclProductB2c> productB2cList = pclProductB2cDao.findByOrderId(orderId);
        HashMap<String, PclProductB2cGoodDefectiveDTO> skuMap = com.alibaba.nacos.shaded.com.google.common.collect.Maps.newHashMap();
        productB2cList.forEach(productB2c -> {
            PclProductB2cGoodDefectiveDTO goodDefectiveDTO = skuMap.get(productB2c.getSku());
            if (goodDefectiveDTO == null) {
                goodDefectiveDTO = new PclProductB2cGoodDefectiveDTO();
                goodDefectiveDTO.setSku(productB2c.getSku());
                goodDefectiveDTO.setGoodProductCount(0);
                goodDefectiveDTO.setDefectiveProductCount(0);
            }
            int productCount = Optional.ofNullable(productB2c.getProductCount()).orElse(0L).intValue();
            if (Objects.equals(ProductInventoryType.DEFECTIVE_PRODUCT.getCode(), productB2c.getInventoryType())) {
                goodDefectiveDTO.setDefectiveProductCount(productCount);
            } else {
                goodDefectiveDTO.setGoodProductCount(productCount);
            }
            skuMap.put(productB2c.getSku(), goodDefectiveDTO);
        });
        return skuMap;
    }

    private void notifyB2cCainiao(PclOrderB2c pclOrderB2c) {
        B2CFulfillmentOrderCallBackBean callBackBean = B2CFulfillmentOrderCallBackBean.builder()
                .referenceNo(pclOrderB2c.getReferenceNo())
                .status(CainiaoOrderCallBackStatus.FORCE_DESTROY_SUCCESS.getStatus())
                .statusMsg(CainiaoOrderCallBackStatus.FORCE_DESTROY_SUCCESS.getDesc()).build();
        Map<String, Object> msg = new HashMap<>();
        msg.put("businessType", 2);
        msg.put("businessTypeMsg", "Fulfillment Order");
        msg.put("orderInfo", callBackBean);
        wallTechKafkaProducer.send(etowerOrderCallbackTopic, String.valueOf(pclOrderB2c.getShipperId()), JSON.toJSONString(msg));
        //记录请求日志
        SysEdiLogCreateDTO sysEdiLogCreateDTO = SysEdiLogCreateDTO.builder()
                .aggregator(SessionContext.getContext().getAggregator())
                .request(JSONObject.toJSONString(msg, SerializerFeature.WriteDateUseDateFormat))
                .requestMethod("MESSAGE")
                .requestUrl(etowerOrderCallbackTopic)
                .shipper(pclOrderB2c.getShipperId())
                .startTime(new Date())
                .endTime(new Date())
                .aggregator(pclOrderB2c.getAggregator())
                .businessType(ThirdApiBusinessType.ETO_B2C.getType())
                .build();
        sysEdiLogService.createEdiLog(sysEdiLogCreateDTO);
    }

    private List<PclOrderB2c> forceDestroyOrder(List<Long> ids, List<SubError> errors) {
        List<PclOrderB2c> pclOrderB2cList = pclOrderB2cDao.selectByIdList(ids);
        //校验状态 只有“已接收”可以销毁自提
        List<PclOrderB2c> list = pclOrderB2cList.stream()
                .filter(p -> !Objects.equals(B2cOrderStatus.CONFIRMED_OUTBOUND.getType(), p.getStatus())).collect(Collectors.toList());
        if (Detect.notEmpty(list)) {
            errors.add(SubError.build(PclOrderB2cResultCode.ORDER_FORCE_DESTROY_STATUS_ERROR));
            return null;
        }
        //校验服务 只有商家自提服务（OFFLINE_PICKUP_TRUCK）可以销毁自提
        List<Long> channelIds = pclOrderB2cList.stream().map(p -> p.getChannelId()).distinct().collect(Collectors.toList());
        List<SysOverseasWarehouseChannel> warehouseChannels = sysOverseasWarehouseChannelDao.selectByIdList(channelIds);
        SysOverseasWarehouseChannel warehouseChannel = warehouseChannels.stream()
                .filter(p -> !Objects.equals("OFFLINE_PICKUP_TRUCK", p.getVirtualChannelName()))
                .findFirst().orElse(null);
        if (warehouseChannel != null) {
            errors.add(SubError.build(PclOrderB2cResultCode.ORDER_FORCE_DESTROY_STATUS_ERROR));
            return null;
        }
        //更新订单
        pclOrderB2cList.forEach(pclOrderB2c -> {
            PclOrderB2c updatePclOrderB2c = new PclOrderB2c();
            updatePclOrderB2c.setId(pclOrderB2c.getId());
            updatePclOrderB2c.setStatus(B2cOrderStatus.OUTBOUND.getType());
            updatePclOrderB2c.setPickUpResult("Destroyed");
            pclOrderB2cDao.updateByPrimaryKeySelective(updatePclOrderB2c);
            //更新商品出库数量为商品数量
            List<PclProductB2c> productB2cList = pclProductB2cDao.findByOrderId(pclOrderB2c.getId());
            productB2cList.forEach(productB2c -> {
                PclProductB2c updateProductB2c = new PclProductB2c();
                updateProductB2c.setId(productB2c.getId());
                updateProductB2c.setOutboundCount(productB2c.getProductCount());
                updateProductB2c.initUpdateEntity();
                pclProductB2cDao.updateByPrimaryKeySelective(updateProductB2c);
            });

            //更新库龄，批次，库存 生成费用
            PclOrderB2cBatchOutboundUpload outboundUpload = new PclOrderB2cBatchOutboundUpload();
            outboundUpload.setOrderNo(pclOrderB2c.getOrderNo());
            outboundUpload.setPclOrderB2c(pclOrderB2c);
            batchOutboundOne(outboundUpload);
        });
        return pclOrderB2cList;
    }

    private List<SubError> createPickUpFee(SysOverseasWarehouseShipperRateDetailDTO pickUpFeeRateConfig, PclOrderB2c pclOrderB2c) {
        Date now = new Date();
        List<SubError> subErrors=new ArrayList<>();
        Long handleId = pickUpFeeRateConfig.getRateId();
        ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, now);
        BigDecimal totalAmount;
        //未配置价卡则费用为0
        if (Detect.notEmpty(rateDetail.getActHandleReturnDetails())) {
            if (pickUpFeeRateConfig.getBillingType() != null && pickUpFeeRateConfig.getBillingType().equals(OverseasWarehouseRateBillingType.ORDER.getType())) {
                totalAmount=calculatePickUpFeeForOrderBillingType(rateDetail, pickUpFeeRateConfig);
            } else {
                totalAmount=calculatePickUpFeeForPalletBillingType(pclOrderB2c,rateDetail, pickUpFeeRateConfig);
            }
            ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
            create.setCostType(OverseasWarehouseCostType.PICK_UP_FEE.getType());
            create.setOrderNo(pclOrderB2c.getOrderNo());
            create.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
            create.setShipperId(pclOrderB2c.getShipperId());
            create.setShipperName(pclOrderB2c.getShipperName());
            create.setAggregator(pclOrderB2c.getAggregator());
            create.setAggregatorName(pclOrderB2c.getAggregatorName());
            create.setAmount(BigDecimalUtils.covertNullToZero(totalAmount).setScale(3, BigDecimal.ROUND_HALF_UP));
            create.setCurrency(pickUpFeeRateConfig.getCurrency());
            create.setSource(BillSource.AUTOMATIC.getType());
            create.setTrackingNo(pclOrderB2c.getTrackingNo());
            create.setReferenceNo(pclOrderB2c.getReferenceNo());
            create.setDiscount(pickUpFeeRateConfig.getDiscount());
            subErrors = actOverseasWarehouseBillService.consumeBill(create);
        }
        return subErrors;
    }

    private BigDecimal calculatePickUpFeeForPalletBillingType(PclOrderB2c pclOrderB2c, ActHandleReturnDetailDTO rateDetail, SysOverseasWarehouseShipperRateDetailDTO pickUpFeeRateConfig) {
        BigDecimal totalAmount;
        BigDecimal standardPalletAmount=BigDecimal.ZERO;
        BigDecimal onnStandardPalletAmount=BigDecimal.ZERO;
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        ActHandleReturnDetail actHandleReturnDetail = actHandleReturnDetails.get(0);
        //标准托盘费用
        BigDecimal standardPalletPrice = actHandleReturnDetail.getStandardPalletPrice();
        Integer standardPalletCount = pclOrderB2c.getStandardPalletCount();
        if(standardPalletPrice!=null&&standardPalletCount!=null){
            standardPalletAmount=standardPalletPrice.multiply(new BigDecimal(standardPalletCount));
        }
        //非标准托盘费用
        BigDecimal nonStandardPalletPrice = actHandleReturnDetail.getNonStandardPalletPrice();
        Integer nonStandardPalletCount = pclOrderB2c.getNonStandardPalletCount();
        if(nonStandardPalletPrice!=null&&nonStandardPalletCount!=null){
            onnStandardPalletAmount=nonStandardPalletPrice.multiply(new BigDecimal(nonStandardPalletCount));
        }
        totalAmount=standardPalletAmount.add(onnStandardPalletAmount);
        return RateCalculateUtils.calculateDiscountedFee(totalAmount, pickUpFeeRateConfig.getDiscount());
    }

    private BigDecimal calculatePickUpFeeForOrderBillingType(ActHandleReturnDetailDTO rateDetail, SysOverseasWarehouseShipperRateDetailDTO pickUpFeeRateConfig) {
        BigDecimal totalAmount;
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        ActHandleReturnDetail actHandleReturnDetail = actHandleReturnDetails.get(0);
        totalAmount = actHandleReturnDetail.getItemPrice();
        return RateCalculateUtils.calculateDiscountedFee(totalAmount, pickUpFeeRateConfig.getDiscount());
    }

    private List<SubError> createPalletFee(SysOverseasWarehouseShipperRateDetailDTO palletFeeConfig, PclOrderB2c pclOrderB2c) {
        Date now = new Date();
        List<SubError> subErrors = Lists.newArrayList();
        Long handleId = palletFeeConfig.getRateId();
        ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, now);
        BigDecimal totalAmount;
        BigDecimal standardPalletAmount=BigDecimal.ZERO;
        BigDecimal onnStandardPalletAmount=BigDecimal.ZERO;
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        if (Detect.empty(actHandleReturnDetails)) {
           return subErrors;
        }
        ActHandleReturnDetail actHandleReturnDetail = actHandleReturnDetails.get(0);
        //标准托盘费用
        BigDecimal standardPalletPrice = actHandleReturnDetail.getStandardPalletPrice();
        Integer standardPalletCount = pclOrderB2c.getStandardPalletCount();
        if(standardPalletPrice!=null&&standardPalletCount!=null){
            standardPalletAmount=standardPalletPrice.multiply(new BigDecimal(standardPalletCount));
        }
        //非标准托盘费用
        BigDecimal nonStandardPalletPrice = actHandleReturnDetail.getNonStandardPalletPrice();
        Integer nonStandardPalletCount = pclOrderB2c.getNonStandardPalletCount();
        if(nonStandardPalletPrice!=null&&nonStandardPalletCount!=null){
            onnStandardPalletAmount=nonStandardPalletPrice.multiply(new BigDecimal(nonStandardPalletCount));
        }
        totalAmount=standardPalletAmount.add(onnStandardPalletAmount);
        BigDecimal discountFee = RateCalculateUtils.calculateDiscountedFee(totalAmount, palletFeeConfig.getDiscount());
        ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
        create.setCostType(OverseasWarehouseCostType.PALLET_FEE.getType());
        create.setOrderNo(pclOrderB2c.getOrderNo());
        create.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
        create.setShipperId(pclOrderB2c.getShipperId());
        create.setShipperName(pclOrderB2c.getShipperName());
        create.setAggregator(pclOrderB2c.getAggregator());
        create.setAggregatorName(pclOrderB2c.getAggregatorName());
        create.setAmount(BigDecimalUtils.covertNullToZero(discountFee).setScale(3, BigDecimal.ROUND_HALF_UP));
        create.setCurrency(palletFeeConfig.getCurrency());
        create.setSource(BillSource.AUTOMATIC.getType());
        create.setTrackingNo(pclOrderB2c.getTrackingNo());
        create.setReferenceNo(pclOrderB2c.getReferenceNo());
        create.setDiscount(palletFeeConfig.getDiscount());
        subErrors = actOverseasWarehouseBillService.consumeBill(create);
        return subErrors;
    }

    private List<SubError> createOrderHandlingFee(SysOverseasWarehouseShipperRateDetailDTO orderHandlingFeeConfig, PclOrderB2c pclOrderB2c) {
        Date now = new Date();
        List<SubError> subErrors = Lists.newArrayList();
        Long handleId = orderHandlingFeeConfig.getRateId();
        ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, now);
        BigDecimal totalAmount;
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        if (Detect.empty(actHandleReturnDetails)) {
         return subErrors;
        }
        ActHandleReturnDetail actHandleReturnDetail = actHandleReturnDetails.get(0);
        totalAmount = actHandleReturnDetail.getItemPrice();
        BigDecimal discountFee = RateCalculateUtils.calculateDiscountedFee(totalAmount, orderHandlingFeeConfig.getDiscount());
        ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
        create.setCostType(OverseasWarehouseCostType.ORDER_HANDLING_FEE.getType());
        create.setOrderNo(pclOrderB2c.getOrderNo());
        create.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
        create.setShipperId(pclOrderB2c.getShipperId());
        create.setShipperName(pclOrderB2c.getShipperName());
        create.setAggregator(pclOrderB2c.getAggregator());
        create.setAggregatorName(pclOrderB2c.getAggregatorName());
        create.setAmount(BigDecimalUtils.covertNullToZero(discountFee).setScale(3, BigDecimal.ROUND_HALF_UP));
        create.setCurrency(orderHandlingFeeConfig.getCurrency());
        create.setSource(BillSource.AUTOMATIC.getType());
        create.setTrackingNo(pclOrderB2c.getTrackingNo());
        create.setReferenceNo(pclOrderB2c.getReferenceNo());
        create.setDiscount(orderHandlingFeeConfig.getDiscount());
        subErrors = actOverseasWarehouseBillService.consumeBill(create);
        return subErrors;
    }

    private List<SubError> createPackingFee(SysOverseasWarehouseShipperRateDetailDTO packingFeeConfig, PclOrderB2c pclOrderB2c) {
        Date now = new Date();
        List<SubError> subErrors=new ArrayList<>();
        Long handleId = packingFeeConfig.getRateId();
        ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, now);
        BigDecimal totalAmount;
        //未配置价卡则费用为0
        if (Detect.notEmpty(rateDetail.getActHandleReturnDetails())) {
            BigDecimal packingSpecificationWeight=pclOrderB2c.getPackingSpecification();
            totalAmount=calculatePackingFeeForSkuBillingType(packingSpecificationWeight, rateDetail, packingFeeConfig);
            //>>>>>记录商品计费明细 start>>>>>
            ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
            create.setCostType(OverseasWarehouseCostType.PACKING_FEE.getType());
            create.setOrderNo(pclOrderB2c.getOrderNo());
            create.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
            create.setShipperId(pclOrderB2c.getShipperId());
            create.setShipperName(pclOrderB2c.getShipperName());
            create.setAggregator(pclOrderB2c.getAggregator());
            create.setAggregatorName(pclOrderB2c.getAggregatorName());
            create.setAmount(BigDecimalUtils.covertNullToZero(totalAmount).setScale(3, BigDecimal.ROUND_HALF_UP));
            create.setCurrency(packingFeeConfig.getCurrency());
            create.setSource(BillSource.AUTOMATIC.getType());
            create.setReferenceNo(pclOrderB2c.getReferenceNo());
            create.setTrackingNo(pclOrderB2c.getTrackingNo());
            create.setDiscount(packingFeeConfig.getDiscount());
            subErrors = actOverseasWarehouseBillService.consumeBill(create);
        }
        return subErrors;
    }

    private BigDecimal calculatePackingFeeForSkuBillingType(BigDecimal packingSpecificationWeight, ActHandleReturnDetailDTO rateDetail, SysOverseasWarehouseShipperRateDetailDTO pickingFeeConfig) {
        if (packingSpecificationWeight == null) {
            log.error("订单包装重量为空，费用为0");
            return BigDecimal.ZERO;
        }
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        ActHandleReturnDetail selectedDetail = null;
        for (ActHandleReturnDetail actHandleReturnDetail : actHandleReturnDetails) {
            BigDecimal weightStart = actHandleReturnDetail.getWeightStart();
            BigDecimal weightEnd = actHandleReturnDetail.getWeightEnd();
            if (weightEnd != null && BigDecimalUtils.gt(packingSpecificationWeight, weightEnd)) {
                continue;
            }
            if (weightStart != null && BigDecimalUtils.le(packingSpecificationWeight, weightStart)) {
                continue;
            }
            selectedDetail = actHandleReturnDetail;
            break;
        }
        if (selectedDetail == null) {
            log.error("订单包装重量没有找到合适的价卡");
            return BigDecimal.ZERO;
        }
        BigDecimal itemPrice = selectedDetail.getItemPrice();
        if (itemPrice == null) {
            log.error("订单包装设置计费单价价卡sku");
            return BigDecimal.ZERO;
        }
        return RateCalculateUtils.calculateDiscountedFee(itemPrice, pickingFeeConfig.getDiscount());
    }

    private List<SubError> createPickingFee(SysOverseasWarehouseShipperRateDetailDTO pickingFeeConfig,List<B2cProductInfo> productInfoList, PclOrderB2c pclOrderB2c) {
        Date now = new Date();
        List<SubError> subErrors=new ArrayList<>();
        Long handleId = pickingFeeConfig.getRateId();
        ActHandleReturnDetailDTO rateDetail = getRateDetail(handleId, now);
        BigDecimal totalAmount = BigDecimal.ZERO;
        //未配置价卡则费用为0
        if (Detect.notEmpty(rateDetail.getActHandleReturnDetails())) {
            if (pickingFeeConfig.getBillingType() != null && pickingFeeConfig.getBillingType().equals(OverseasWarehouseRateBillingType.SKU.getType())) {
                totalAmount=calculatePickFeeForSkuBillingType(productInfoList, rateDetail, pickingFeeConfig);
            } else {
                for (B2cProductInfo productInfo : productInfoList) {
                    BigDecimal amount = calculatePickFeeForItemBillingType(productInfo, rateDetail, pickingFeeConfig);
                    totalAmount = totalAmount.add(amount);
                }
            }
            ActOverseasWarehouseCostCreate create = new ActOverseasWarehouseCostCreate();
            create.setCostType(OverseasWarehouseCostType.PICKING_FEE.getType());
            create.setOrderNo(pclOrderB2c.getOrderNo());
            create.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
            create.setShipperId(pclOrderB2c.getShipperId());
            create.setShipperName(pclOrderB2c.getShipperName());
            create.setAggregator(pclOrderB2c.getAggregator());
            create.setAggregatorName(pclOrderB2c.getAggregatorName());
            create.setAmount(BigDecimalUtils.covertNullToZero(totalAmount).setScale(3, BigDecimal.ROUND_HALF_UP));
            create.setCurrency(pickingFeeConfig.getCurrency());
            create.setSource(BillSource.AUTOMATIC.getType());
            create.setTrackingNo(pclOrderB2c.getTrackingNo());
            create.setReferenceNo(pclOrderB2c.getReferenceNo());
            create.setDiscount(pickingFeeConfig.getDiscount());
            subErrors = actOverseasWarehouseBillService.consumeBill(create);
        }
        return subErrors;
    }

    private BigDecimal calculatePickFeeForItemBillingType(B2cProductInfo productInfo, ActHandleReturnDetailDTO rateDetail, SysOverseasWarehouseShipperRateDetailDTO pickingFeeConfig) {
        BigDecimal weight = productInfo.getGrossWeightActual();
        if (weight == null) {
            log.error("商品详情重量为空sku: {}", productInfo.getSku());
            return BigDecimal.ZERO;
        }
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        ActHandleReturnDetail selectedDetail = null;
        for (ActHandleReturnDetail actHandleReturnDetail : actHandleReturnDetails) {
            BigDecimal weightStart = actHandleReturnDetail.getWeightStart();
            BigDecimal weightEnd = actHandleReturnDetail.getWeightEnd();
            if (weightEnd != null && BigDecimalUtils.gt(weight, weightEnd)) {
                continue;
            }
            if (weightStart != null && BigDecimalUtils.le(weight, weightStart)) {
                continue;
            }
            selectedDetail = actHandleReturnDetail;
            break;
        }
        if (selectedDetail == null) {
            log.error("该商品重量没有找到合适的价卡sku: {}", productInfo.getSku());
            return BigDecimal.ZERO;
        }
        BigDecimal itemPrice = selectedDetail.getItemPrice();
        if (itemPrice == null) {
            log.error("没有设置计费单价价卡sku: {}", productInfo.getSku());
            return BigDecimal.ZERO;
        }
        Long productCount = productInfo.getProductCount();
        if (productCount == null) {
            log.warn("sku商品数量为null");
            productCount = 1L;
        }
        BigDecimal amount = itemPrice.multiply(new BigDecimal(productCount));
        return RateCalculateUtils.calculateDiscountedFee(amount, pickingFeeConfig.getDiscount());
    }

    private BigDecimal calculatePickFeeForSkuBillingType(List<B2cProductInfo> productInfoList, ActHandleReturnDetailDTO rateDetail, SysOverseasWarehouseShipperRateDetailDTO pickingFeeConfig) {
        BigDecimal totalAmount;
        List<ActHandleReturnDetail> actHandleReturnDetails = rateDetail.getActHandleReturnDetails();
        ActHandleReturnDetail actHandleReturnDetail = actHandleReturnDetails.get(0);
        Map<String, List<B2cProductInfo>> groupBySkuMap = productInfoList.stream().collect(Collectors.groupingBy(B2cProductInfo::getSku));
        int size = groupBySkuMap.size();
        BigDecimal itemPrice = actHandleReturnDetail.getItemPrice();
        totalAmount=itemPrice.multiply(new BigDecimal(size));
        return RateCalculateUtils.calculateDiscountedFee(totalAmount, pickingFeeConfig.getDiscount());
    }

    private ActOverseasWarehouseBillItemDTO getActOverseasWarehouseBillItemByType(BigDecimal amount,BigDecimal weight,String currency,Short lastMileRateType,Short lastMileRateDetailType,String feeNameCn,String feeNameEn) {
        ActOverseasWarehouseBillItemDTO billItemDTO = new ActOverseasWarehouseBillItemDTO();
        billItemDTO.setCostType(lastMileRateType);
        billItemDTO.setCostSubType(lastMileRateDetailType);
        billItemDTO.setWeight(weight);
        billItemDTO.setAmount(BigDecimalUtils.covertNullToZero(amount).setScale(3, RoundingMode.HALF_UP));
        billItemDTO.setCurrency(currency);
        billItemDTO.setDiscountedAmount(BigDecimalUtils.covertNullToZero(amount).setScale(3, RoundingMode.HALF_UP));
        if(feeNameCn!=null){
            billItemDTO.setFeeNameCn(feeNameCn);
        }
        if(feeNameEn!=null){
            billItemDTO.setFeeNameEn(feeNameEn);
        }
        return billItemDTO;
    }

    private B2cRateInquiryRequestDTO convertB2cRateInquiryRequestDTOForOutbound(PclOrderB2c pclOrderB2c,List<PclOrderB2cProductDTO> b2cProductList,SysOverseasWarehouseChannelDTO sysOverseasWarehouseChannelDTO) {
        String recipientAddressId = pclOrderB2c.getRecipientAddressId();
        SysAddress sysAddress = sysAddressService.get(recipientAddressId);
        SysOverseasWarehouseChannel sysOverseasWarehouseChannel = sysOverseasWarehouseChannelDao.selectByPrimaryKey(pclOrderB2c.getChannelId());
        B2cRateInquiryRequestDTO b2cRateInquiryRequestDTO=new B2cRateInquiryRequestDTO();
        b2cRateInquiryRequestDTO.setServiceCode(sysOverseasWarehouseChannel.getChannelCode());
        //传对接仓代码
        SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(pclOrderB2c.getShipperWarehouseCode(), pclOrderB2c.getAggregator());
        b2cRateInquiryRequestDTO.setFacility(sysOverseasWarehouse != null ? sysOverseasWarehouse.getThirdWarehouseCode() : null);
        b2cRateInquiryRequestDTO.setWeightUnit(WeightUnit.KG.getUnit());
        b2cRateInquiryRequestDTO.setDimensionUnit(DimensionUnit.CM.getUnit());
        b2cRateInquiryRequestDTO.setPostcode(sysAddress.getPostCode());
        b2cRateInquiryRequestDTO.setCity(sysAddress.getCity());
        b2cRateInquiryRequestDTO.setCountry(sysAddress.getCountry());
        b2cRateInquiryRequestDTO.setState(sysAddress.getState());
        CostCalculationPackageDTO costCalculationPackageDTO = getLastMileBillingInfoByProductList(b2cProductList);
        b2cRateInquiryRequestDTO.setWeight(costCalculationPackageDTO.getWeight());
        b2cRateInquiryRequestDTO.setLength(costCalculationPackageDTO.getLength());
        b2cRateInquiryRequestDTO.setWidth(costCalculationPackageDTO.getWidth());
        b2cRateInquiryRequestDTO.setHeight(costCalculationPackageDTO.getHeight());
        b2cRateInquiryRequestDTO.setServiceOption(sysOverseasWarehouseChannelDTO.getServiceOption());
        List<B2cRateInquiryPiecesDTO>pieces=new ArrayList<>();
        b2cRateInquiryRequestDTO.setPieces(pieces);
        return b2cRateInquiryRequestDTO;
    }

    private CostCalculationPackageDTO getLastMileBillingInfoByProductList(List<PclOrderB2cProductDTO> b2cProductList) {
        CostCalculationPackageDTO costCalculationPackageDTO=new CostCalculationPackageDTO();
        BigDecimal totalWeight = b2cProductList.stream().map(PclOrderB2cProductDTO::getGrossWeightActual).reduce(BigDecimal.ZERO, BigDecimal::add);
        costCalculationPackageDTO.setWeight(totalWeight);
        BigDecimal length=getPackageLength(b2cProductList);
        BigDecimal width=getPackageWidth(b2cProductList);
        BigDecimal height=getPackageHeight(b2cProductList);
        costCalculationPackageDTO.setHeight(height);
        costCalculationPackageDTO.setWidth(width);
        costCalculationPackageDTO.setLength(length);
        return costCalculationPackageDTO;
    }

    private BigDecimal getPackageHeight(List<PclOrderB2cProductDTO> b2cProductList) {
        BigDecimal totalShortestEdge = BigDecimal.ZERO;
        for (PclOrderB2cProductDTO product : b2cProductList) {
            BigDecimal shortestEdge = product.getProductLengthActual().min(product.getProductWidthActual()).min(product.getProductHeightActual());
            totalShortestEdge = totalShortestEdge.add(shortestEdge);
        }
        return totalShortestEdge;
    }

    private BigDecimal getPackageWidth(List<PclOrderB2cProductDTO> b2cProductList) {
        BigDecimal longestEdge = getPackageLength(b2cProductList);
        BigDecimal secondLongestEdge = BigDecimal.ZERO;
        for (PclOrderB2cProductDTO product : b2cProductList) {
            BigDecimal currentSecondLongest = product.getProductLengthActual().min(product.getProductWidthActual())
                    .max(product.getProductWidthActual().min(product.getProductHeightActual()));
            if (currentSecondLongest.compareTo(secondLongestEdge) > 0 && currentSecondLongest.compareTo(longestEdge) < 0) {
                secondLongestEdge = currentSecondLongest;
            }
        }
        return secondLongestEdge;
    }

    private BigDecimal getPackageLength(List<PclOrderB2cProductDTO> b2cProductList) {
        BigDecimal longestEdge = BigDecimal.ZERO;
        for (PclOrderB2cProductDTO product : b2cProductList) {
            BigDecimal currentLongest = product.getProductLengthActual().max(product.getProductWidthActual()).max(product.getProductHeightActual());
            if (currentLongest.compareTo(longestEdge) > 0) {
                longestEdge = currentLongest;
            }
        }
        return longestEdge;
    }

    private void updateItems(B2cOrderDispatchRequest.B2cOrderDispatchRequestItem item) {
        PclProductB2c pclProductB2c=new PclProductB2c();
        pclProductB2c.initUpdateEntity();
        pclProductB2c.setId(item.getProductId());
        pclProductB2c.setOutboundCount(item.getProductQty().longValue());
        pclProductB2cDao.updateByPrimaryKeySelective(pclProductB2c);
    }


    @Override
    public List<SubError> batchOutbound(List<PclOrderB2cBatchOutboundUpload> list) {
        if (CollectionUtils.isEmpty(list)) {
            return Collections.emptyList();
        }
        List<SubError> errors = valide(list);
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        for (PclOrderB2cBatchOutboundUpload dto : list) {
            pclOrderB2cService.batchOutboundOne(dto);
        }
        return errors;
    }

    /**
     *封装批量出库为单个出库
     * @param dto
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void batchOutboundOne(PclOrderB2cBatchOutboundUpload dto) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        PclOrderB2c order = dto.getPclOrderB2c();
        Long orderId = order.getId();
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();
        orderEventService.packageEvents(order.getOrderNo(),order.getReferenceNo(),order.getId(),events, WareHouseOrderEventCode.OUT_OF_STORAGE);
        PclOrderB2c orderB2c = new PclOrderB2c();
        orderB2c.setId(orderId);
        orderB2c.setDateShipping(new Date());
        orderB2c.setStatus(B2cOrderStatus.OUTBOUND.getType());
        orderB2c.setTrackingNo(dto.getTrackingNo());
        int i = pclOrderB2cDao.updateByPrimaryKeySelective(orderB2c);
        createBatchOutboundLog(orderB2c);
        List<PclProductB2c> produs = getProducts(orderId);
        produs.forEach(productB2c -> {
            PclProductB2c pclProductB2c = new PclProductB2c();
            pclProductB2c.setId(productB2c.getId());
            //全量出库 出库数量等于预报数量
            pclProductB2c.setOutboundCount(productB2c.getProductCount());
            pclProductB2cDao.updateByPrimaryKeySelective(pclProductB2c);
        });
        //订单全量出库扣减库存，库龄和批次更新
        pclOrderB2cService.handleBatchAndInventory(order);
        /*for (PclProductB2c pclProductB2c : produs) {
            Integer shipperId = order.getShipperId();
            String sku = pclProductB2c.getSku();
            String warehouseCode = pclProductB2c.getOverseasWarehouseCode();
            PclBatch batchForForecastCount = getBatchData(orderId, sku, warehouseCode);
            if (batchForForecastCount == null) {
                log.warn("{} {} {} have no batch", orderId, sku, warehouseCode);
                continue;
            }
            log.info("{} {} {} 预报： {}", orderId, sku, warehouseCode, batchForForecastCount.getForecastAmount());
            // 更新库龄数据
            removeAgingCount(sku, warehouseCode, shipperId, batchForForecastCount.getForecastAmount(), order);
            // 更新待出库商品数量  待出库的商品减少了 productQty 个
            PclInventory reqCondition = new PclInventory();
            reqCondition.setSku(sku);
            reqCondition.setOverseasWarehouseCode(warehouseCode);
            final PclInventory inventory = pclInventoryDao.selectLimitOne(reqCondition);
            if (inventory == null) {
                log.error("not find the inventory record with sku:{} and overseas warehouse:{}", sku, warehouseCode);
            } else {
                PclInventory waitForUpdate = new PclInventory();
                int count = inventory.getOutgoingInventory() == null ? 0 : inventory.getOutgoingInventory();
                int forecastCount = batchForForecastCount == null ? 0 : batchForForecastCount.getForecastAmount();
                final int outGoingCount = count - forecastCount;
                waitForUpdate.setOutgoingInventory(outGoingCount);
                log.info("{} 更新待出库数量: {} - {} = {}", inventory.getId(), count, forecastCount, outGoingCount);
                int availableCount = inventory.getAvailableInventory() == null ? 0 : inventory.getAvailableInventory();
                final int availableCountAfterCal = availableCount - (forecastCount);
                // waitForUpdate.setAvailableInventory(availableCountAfterCal);
                log.info("{} 更新可用数量: {} - ({}) = {}", inventory.getId(), availableCount,
                        forecastCount, availableCountAfterCal);
                waitForUpdate.setId(inventory.getId());
                waitForUpdate.initUpdateEntity();
                pclInventoryDao.outgoingInventoryCount(waitForUpdate, 0, -1*forecastCount);
            }
            // 更新 批次状态 和 变更的数量
            PclBatch pclBatch = new PclBatch();
            pclBatch.setOrderId(orderId);
            pclBatch.setStatus(BatchStatus.OUTBOUND.getStatus());
            pclBatch.setWarehousingAmount(batchForForecastCount.getForecastAmount());
            Example example = getSearchConditionForBatchData(orderId, sku, warehouseCode);
            pclBatchDao.updateByExampleSelective(pclBatch, example);
        }*/
        orderEventService.addEventsByApi(events);
        //计费 组装数据 和单个手动出库一致
        List<SysProduct> sysSkuList = sysProductDao.findBySkuList(produs.stream().map(p -> p.getSku()).collect(Collectors.toList()), order.getShipperId(), ProductType.OVERSEASWAREHOUSE.getType());
        Map<String, Long> skuQtyMap = produs.stream().collect(Collectors.toMap(PclProductB2c::getSku, PclProductB2c::getProductCount, (o1, o2) -> o2));
        List<PclOrderB2cProductDTO> b2cProductList = sysSkuList.stream().map(sysSku ->{
            PclOrderB2cProductDTO b2cProductDTO = new PclOrderB2cProductDTO();
            b2cProductDTO.setSku(sysSku.getSku());
            Long productQty = skuQtyMap.get(sysSku.getSku());
            b2cProductDTO.setQuantity(productQty != null ? productQty.intValue() : 0);
            b2cProductDTO.setGrossWeightActual(sysSku.getGrossWeightActual());
            b2cProductDTO.setProductHeightActual(sysSku.getProductHeightActual());
            b2cProductDTO.setProductLengthActual(sysSku.getProductLengthActual());
            b2cProductDTO.setProductWidthActual(sysSku.getProductWidthActual());
            return b2cProductDTO;
        }).collect(Collectors.toList());
        //特殊需求 要求写死 宁兴优贝 集成商 使用运费测算接口获取运费 16028正式环境id 15355 qa环境id
//        if (Objects.equals(aggregator, 16028) || Objects.equals(aggregator, 15355)) {
//            //需要进行接口调用获取派送费
//            calculateLastMileFeeByRateInquiry(order, b2cProductList);
//        } else {
//
//        }
        PclOrderB2cOrderDTO pclOrderB2cOrderDTO = new PclOrderB2cOrderDTO();
        pclOrderB2cOrderDTO.setProducts(b2cProductList);
//        lastMileCalculateFeeService.calculateLastMileFee(order, pclOrderB2cOrderDTO, false);
//        //生成出库杂费
//        Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> rateDetailMap = sysOverseasWarehouseShipperService.getOutboundMiscellaneousRateConfigInfo(order.getShipperId(), order.getShipperWarehouseCode(), order.getChannelId());
//        if (Detect.notEmpty(rateDetailMap)) {
//            List<B2cProductInfo> productInfoList = pclProductB2cDao.getProductSkuCountInfo(Collections.singletonList(order.getOrderNo()));
//            createOutboundMiscellaneousFeeForOutbound(order, rateDetailMap, productInfoList);
//        }
        //生成费用
        triggerOutboundFee(order, pclOrderB2cOrderDTO);
    }

    private void createBatchOutboundLog(PclOrderB2c orderB2c) {
        SysOperationLog sysOperationLog=new SysOperationLog();
        sysOperationLog.setMethod("b2c/order/batchOutbound");
        sysOperationLog.setOperationDescription("海外仓订单批量出库");
        sysOperationLog.setDataId(orderB2c.getId());
        String shippingDateStr = DateUtils.asDateTimeStr(orderB2c.getDateShipping(),DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1);
        StringBuilder changeContent = new StringBuilder();
        changeContent.append("订单状态：").append("已接收").append("—>").append("已出库").append("。\n");
        changeContent.append("出库时间：").append("空值").append("—>").append(shippingDateStr).append("。\n");
        changeContent.append("跟踪号：").append("空值").append("—>").append(orderB2c.getTrackingNo()).append("。\n");
        sysOperationLog.setChangeContent(changeContent.toString());
        sysOperationLogService.addOperationLog(sysOperationLog);
    }

    @Override
    public List<PclOrderB2cVO> queryShipperOutboundOrder(Integer shipperId) {
        return pclOrderB2cDao.queryShipperOutboundOrder(shipperId,B2cOrderStatus.OUTBOUND.getType());
    }

    private PclBatch getBatchData(Long orderId, String sku, String warehouseCode) {
        Example example = new Example.Builder(PclBatch.class)
                .andWhere(
                        Sqls.custom()
                                .andEqualTo("orderId", orderId)
                                .andEqualTo("sku", sku)
                                .andEqualTo("overseasWarehouseCode", warehouseCode)
                ).build();
        List<PclBatch> pclBatches = pclBatchDao.selectByExample(example);
        if (pclBatches.size() == 0) {
            return null;
        }
        return pclBatches.get(0);
    }

    private List<PclProductB2c> getProducts(Long orderId) {
        Example example = new Example.Builder(PclProductB2c.class)
                .andWhere(
                        Sqls.custom().andEqualTo("orderId", orderId)
                ).build();
        return pclProductB2cDao.selectByExample(example);
    }

    private List<SubError> valide(List<PclOrderB2cBatchOutboundUpload> list) {
        List<SubError> errors = new ArrayList<>();
        List<String> orderNos = new ArrayList<>();
        for (PclOrderB2cBatchOutboundUpload dto : list) {
            if (StringUtils.isEmpty(dto.getOrderNo())) {
                errors.add(SubError.build(PclOrderB2cResultCode.OUTBOUND_NO_ORDER_NO_ERROR, dto.getRowNum()));
            }
            if (StringUtils.isEmpty(dto.getTrackingNo())) {
                errors.add(SubError.build(PclOrderB2cResultCode.OUTBOUND_NO_TRACKING_NO_ERROR, dto.getRowNum(), dto.getOrderNo()));
            }
            orderNos.add(dto.getOrderNo());
        }
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors;
        }
        Example pclOrderB2cExample = new Example.Builder(PclOrderB2c.class)
                .andWhere(
                        Sqls.custom().andIn("orderNo", orderNos)
                ).build();

        List<PclOrderB2c> pclOrderB2cs = pclOrderB2cDao.selectByExample(pclOrderB2cExample);
        Map<String, List<PclOrderB2c>> collect = pclOrderB2cs.stream().collect(Collectors.groupingBy(PclOrderB2c::getOrderNo));
        for (PclOrderB2cBatchOutboundUpload dto : list) {
            List<PclOrderB2c> lll = collect.get(dto.getOrderNo());
            if (CollectionUtils.isEmpty(lll)) {
                errors.add(SubError.build(PclOrderB2cResultCode.OUTBOUND_ORDER_NOT_EXIST_ERROR, dto.getRowNum(), dto.getOrderNo()));
            } else {
                PclOrderB2c pclOrderB2c = lll.get(0);
                if (pclOrderB2c == null) {
                    errors.add(SubError.build(PclOrderB2cResultCode.OUTBOUND_ORDER_NOT_EXIST_ERROR, dto.getRowNum(), dto.getOrderNo()));
                } else if (!Objects.equals(pclOrderB2c.getStatus(), B2cOrderStatus.CONFIRMED_OUTBOUND.getType())) {
                    errors.add(SubError.build(PclOrderB2cResultCode.OUTBOUND_ORDER_STATUS_ERROR, dto.getRowNum(), dto.getOrderNo()));
                } else {
                    dto.setPclOrderB2c(pclOrderB2c);
                }
            }
        }
        return errors;
    }

//    private int removeAgingCount(String sku, String warehouseCode, Integer shipperId, Integer skuCount,PclOrderB2c pclOrderB2c) {
//        int i = 0;
//        List<PclInventoryAging> pclInventoryAgingList = pclInventoryAgingDao.findByShipperAndSkuAndWarehouseCode(shipperId, warehouseCode, sku);
//        if (pclInventoryAgingList == null || pclInventoryAgingList.size() == 0) {
//            log.info("there is no inventory aging data to update for sku:{}, warehouse code:{}, count:{},shipper:{}", sku, warehouseCode, skuCount, shipperId);
//            return i;
//        }
//        //被扣减的库龄
//        List<PclInventoryAging> inventoryAgingList = Lists.newArrayList();
//        //按照库龄 优先库存时间久的。记录下需扣减的库龄数据，出库后扣除
//        for (PclInventoryAging pclInventoryAging : pclInventoryAgingList) {
//            pclInventoryAging.initUpdateEntity();
//            Integer currentCount = pclInventoryAging.getCurrentCount();
//            if (currentCount == null) {
//                continue;
//            }
//            PclInventoryAging waitForUpdate = new PclInventoryAging();
//            waitForUpdate.setId(pclInventoryAging.getId());
//            waitForUpdate.initUpdateEntity();
//            if (currentCount != 0) {
//                if (currentCount.longValue() < skuCount) {
//                    waitForUpdate.setCurrentCount(0);
//                    waitForUpdate.setRemainingCount(0);
//                    skuCount = skuCount - currentCount;
//                    i += pclInventoryAgingDao.updateByPrimaryKeySelective(waitForUpdate);
//                    inventoryAgingList.add(pclInventoryAging);
//                } else if (currentCount.longValue() == skuCount) {
//                    waitForUpdate.setCurrentCount(0);
//                    waitForUpdate.setRemainingCount(0);
//                    i += pclInventoryAgingDao.updateByPrimaryKeySelective(waitForUpdate);
//                    inventoryAgingList.add(pclInventoryAging);
//                    break;
//                } else {
//                    //获取本订单下，该库龄的确认发送是生成的log日志
//                    List<PclInventoryLog>pclInventoryLogList= pclInventoryLogDao.getByOrderIdAndAgingId(pclOrderB2c.getId(),pclInventoryAging.getId());
//                    if(Detect.notEmpty(pclInventoryLogList)){
//                        waitForUpdate.setCurrentCount(currentCount - skuCount);
//                        Integer totalChangeCount = pclInventoryLogList.stream().map(PclInventoryLog::getChangeCount).reduce(0, Integer::sum);
//                        if(totalChangeCount>skuCount){
//                            //获取实际出库数量和log中的出库数量的差值
//                            //实际出库数量少于log中的出库数量，则需要将改库龄中的remainCount补上
//                            int differenceCount = totalChangeCount - skuCount;
//                            waitForUpdate.setRemainingCount(pclInventoryAging.getRemainingCount()+differenceCount);
//                        }else{
//                            //如果实际出库数量大于log中的出库数量。则需要将remainCount减去差值
//                            int differenceCount = skuCount - totalChangeCount;
//                            waitForUpdate.setRemainingCount(pclInventoryAging.getRemainingCount()-differenceCount);
//                        }
//                        i += pclInventoryAgingDao.updateByPrimaryKeySelective(waitForUpdate);
//                        inventoryAgingList.add(pclInventoryAging);
//                        break;
//                    }
//                }
//            }
//        }
//        //记录代发订单和入库单的关联关系
//        orderPoRelationService.recordOrderB2cRelation(pclOrderB2c, inventoryAgingList);
//        return i;
//    }

    private void removeAgingCount(String sku, String warehouseCode, Integer shipperId, Integer skuCount,PclOrderB2c pclOrderB2c) {
        //根据确认发送记录的库存日志 扣除库龄数量
        List<PclInventoryLogDTO> pclInventoryLogDTOList = pclInventoryLogDao.getDetailByOrderIdAndSku(pclOrderB2c.getId(), sku);
        Integer logCount = pclInventoryLogDTOList.stream().map(p -> p.getChangeCount()).filter(p -> p != null).reduce(0, Integer::sum);
        //被扣减的库龄
        List<PclInventoryAging> inventoryAgingList = Lists.newArrayList();
        if (skuCount == logCount) {
            log.info("实际出库数量等于确认发送数量，直接扣除日志占用的库龄");
            // 实际出库等于确认发送
            pclInventoryLogDTOList.forEach(logDTO -> {
                PclInventoryAging pclInventoryAging = pclInventoryAgingDao.selectByPrimaryKey(logDTO.getInventoryAgingId());
                if(pclInventoryAging==null){
                    log.error("库龄：{}，未找到，数据出现问题",logDTO.getInventoryAgingId());
                    return;
                }
                PclInventoryAging updateAging = new PclInventoryAging();
                updateAging.setId(pclInventoryAging.getId());
                updateAging.setCurrentCount(pclInventoryAging.getCurrentCount() - logDTO.getChangeCount());
                updateAging.initUpdateEntity();
                pclInventoryAgingDao.updateByPrimaryKeySelective(updateAging);
                inventoryAgingList.add(pclInventoryAging);
            });
        } else if (skuCount > logCount) {
            log.info("实际出库数量比确认发送数量多，先扣除日志占用的库龄，再扣除上架时间最远的库龄");
            Integer extraCount = skuCount - logCount;
            HashMap<Long, PclInventoryAging> updateAgingMap = new HashMap<>();
            pclInventoryLogDTOList.forEach(logDTO -> {
                PclInventoryAging pclInventoryAging = pclInventoryAgingDao.selectByPrimaryKey(logDTO.getInventoryAgingId());
                if (pclInventoryAging == null) {
                    log.error("库龄：{}，未找到，数据出现问题", logDTO.getInventoryAgingId());
                    return;
                }
                Integer remainingCount = Optional.ofNullable(pclInventoryAging.getRemainingCount()).orElse(0);
                PclInventoryAging updateAging = new PclInventoryAging();
                updateAging.setId(pclInventoryAging.getId());
                updateAging.setCurrentCount(pclInventoryAging.getCurrentCount() - logDTO.getChangeCount());
                updateAging.initUpdateEntity();
                updateAgingMap.put(pclInventoryAging.getId(), updateAging);
//                pclInventoryAgingDao.updateByPrimaryKeySelective(updateAging);
                inventoryAgingList.add(pclInventoryAging);
            });
            //处理多出的数量

            List<PclInventoryAging> allAgingList = pclInventoryAgingDao.findByShipperAndSkuAndWarehouseCode(shipperId, warehouseCode, sku);
            allAgingList = allAgingList.stream().filter(p -> p.getRemainingCount() > 0).collect(Collectors.toList());
            allAgingList.sort(Comparator.comparing(PclInventoryAging::getPutAwayDate));
            for (PclInventoryAging aging : allAgingList) {
                if (extraCount == 0) {
                    break;
                }
                PclInventoryAging existUpdateAging = updateAgingMap.get(aging.getId());
                Integer remainingCount = aging.getRemainingCount();
                PclInventoryAging updateAging = new PclInventoryAging();
                updateAging.setId(aging.getId());
                updateAging.initUpdateEntity();
                if (extraCount >= remainingCount) {
                    //多出的数量 大于等于 剩余数量
                    if (existUpdateAging != null) {
                        existUpdateAging.setCurrentCount(existUpdateAging.getCurrentCount() - remainingCount);
                        existUpdateAging.setRemainingCount(0);
                    } else {
                        updateAging.setCurrentCount(aging.getCurrentCount() - remainingCount);
                        updateAging.setRemainingCount(0);
                        updateAgingMap.put(aging.getId(), updateAging);
                        inventoryAgingList.add(BeanUtils.transform(aging, PclInventoryAging.class));
                    }
                    extraCount = extraCount - remainingCount;
                } else {
                    //多出的数量 小于 剩余数量
                    if (existUpdateAging != null) {
                        existUpdateAging.setCurrentCount(existUpdateAging.getCurrentCount() - extraCount);
                        existUpdateAging.setRemainingCount(remainingCount - extraCount);
                    } else {
                        updateAging.setCurrentCount(aging.getCurrentCount() - extraCount);
                        updateAging.setRemainingCount(remainingCount - extraCount);
                        updateAgingMap.put(aging.getId(), updateAging);
                        inventoryAgingList.add(aging);
                    }
                    extraCount = 0;
                }
            }
            //批量更新
            updateAgingMap.values().stream().forEach(updateAging -> {
                pclInventoryAgingDao.updateByPrimaryKeySelective(updateAging);
            });

        } else if (skuCount < logCount) {
            log.info("实际出库数量比确认发送数量少 恢复被占用的数量（remaining_count）");
            pclInventoryLogDTOList.stream().collect(Collectors.toList()).sort(Comparator.comparing(PclInventoryLogDTO::getPutAwayDate));
            for (PclInventoryLogDTO logDTO : pclInventoryLogDTOList) {
                Integer changeCount = Optional.ofNullable(logDTO.getChangeCount()).orElse(0);
                PclInventoryAging pclInventoryAging = pclInventoryAgingDao.selectByPrimaryKey(logDTO.getInventoryAgingId());
                if (pclInventoryAging == null) {
                    log.error("库龄ID：{}，未找到，数据出现问题", logDTO.getInventoryAgingId());
                    continue;
                }
                PclInventoryAging updateAging = new PclInventoryAging();
                updateAging.setId(pclInventoryAging.getId());
                if (skuCount > 0) {
                    //扣减库龄
                    if (skuCount >= changeCount) {
                        //实际数量 大于等于 变更数量
                        updateAging.setCurrentCount(pclInventoryAging.getCurrentCount() - changeCount);
                        updateAging.initUpdateEntity();
                        skuCount =- logDTO.getChangeCount();
                    } else {
                        //实际数量 小于 变更数量
                        updateAging.setCurrentCount(pclInventoryAging.getCurrentCount() - skuCount);
                        //恢复被占用的数量
                        updateAging.setRemainingCount(pclInventoryAging.getRemainingCount() + (changeCount - skuCount));
                        updateAging.initUpdateEntity();
                        skuCount = 0;
                    }
                    inventoryAgingList.add(pclInventoryAging);
                } else {
                    // 无需扣减库龄 只是恢复剩余数量
                    updateAging.setRemainingCount(pclInventoryAging.getRemainingCount() + changeCount);
                    updateAging.initUpdateEntity();
                }
                pclInventoryAgingDao.updateByPrimaryKeySelective(updateAging);
            }
        }
        //记录代发订单和入库单的关联关系
        orderPoRelationService.recordOrderB2cRelation(pclOrderB2c, inventoryAgingList);
    }
    private PclBatch getBatchBySkuAndOrderAndWareshouse(Long orderId, String sku, String warehouseCode) {
        Example example = getSearchConditionForBatchData(orderId, sku, warehouseCode);
        final List<PclBatch> pclBatches = pclBatchDao.selectByExample(example);
        if (pclBatches.size() == 0) {
            log.error("not find the pcl batch for sku:{} orderId:{} warehouseCode:{}", sku, orderId, warehouseCode);
            return null;
        } else if (pclBatches.size() > 1) {
            log.error("find too many record pcl batch for sku:{} orderId:{} warehouseCode:{}", sku, orderId, warehouseCode);
        }
        return pclBatches.get(0);
    }

    private Example getSearchConditionForBatchData(Long orderId, String sku, String warehouseCode) {
        return Example.builder(PclBatch.class)
                .where(Sqls
                        .custom()
                        .andEqualTo("orderId", orderId)
                        .andEqualTo("sku", sku)
                        .andEqualTo("overseasWarehouseCode", warehouseCode)
                ).build();
    }

    @Override
    public Integer getOrderPlatform(Long orderId) {
        if (orderId == null) {
            return -1;
        }
        return pclOrderB2cDao.getPlatformByOrderId(orderId);
    }

    @Override
    public List<SubError> spiteOrder(B2cOrderSplitRequest req) {
        log.info("拆单请求参数:{}",JSON.toJSONString(req));
        List<SubError> errors = Lists.newArrayList();
        int newOrderCount = 0;
        newOrderCount = req.getItems().get(0).getDetails().size();
        PclOrderB2c originOrder = getOrderById(req.getOrderId());
        if (originOrder == null) {
            log.error("not find the spit order id: {}", req.getOrderId());
            errors.add(SubError.build(PclOrderB2cResultCode.ORDER_DOES_NOT_EXIST_ERROR));
            return errors;
        }
        Short status = originOrder.getStatus();
        if (status != null && !(status.equals(B2cOrderStatus.UNCONFIRMED.getType()) || status.equals(B2cOrderStatus.SENDING_ABNORMALITY.getType()))) {
            errors.add(SubError.build(PclOrderB2cResultCode.ORDER_STATUS_CANNOT_SPLIT_ORDER));
            return errors;
        }
        log.info("将拆分的原始订单 id:{}, orderNo:{}", originOrder.getServiceId(), originOrder.getOrderNo());
        PartyVo shipperInfo = sysPartyService.findByPartyId(originOrder.getShipperId());
        Map<Long, PclProductB2c> productB2cMap = getProductMap(req.getOrderId());
        List<B2cOrderSplitVO> waitForSave = new ArrayList<>();
        boolean haveRemain = false;
        for (int i = 0; i < newOrderCount; i++) {
            B2cOrderSplitVO vo = addSplitOrder(i, originOrder, req, productB2cMap, shipperInfo);
            waitForSave.add(vo);
            if (vo.isHaveRemain()) {
                haveRemain = true;
            }
        }
        if (haveRemain) {
            B2cOrderSplitVO vo = addRemainOrder(originOrder, productB2cMap, req, newOrderCount + 1, shipperInfo);
            waitForSave.add(vo);
        } else {
            log.info("have no remain to order");
        }

        if (Detect.notEmpty(waitForSave)) {
            log.info("过滤拆分后子订单下商品为空的子订单，不保存");
            waitForSave = waitForSave.stream().filter(p -> Detect.notEmpty(p.getProductB2cs())).collect(Collectors.toList());
            log.info("拆分后订单商品:{}", JSON.toJSONString(waitForSave));
            log.info("第一个商品不为空的子订单去覆盖原订单");
            B2cOrderSplitVO firstB2cOrderSplitVO = waitForSave.get(0);
            PclOrderB2c firstOrderB2c = firstB2cOrderSplitVO.getOrderB2c();
            firstOrderB2c.setId(originOrder.getId());
            firstOrderB2c.setOrderNo(originOrder.getOrderNo());
            firstOrderB2c.setStatus(originOrder.getStatus());
            firstB2cOrderSplitVO.setOrderB2c(firstOrderB2c);
            waitForSave.set(0, firstB2cOrderSplitVO);
            pclOrderB2cService.doSaveSplitOrder(new ArrayList<>(productB2cMap.keySet()), waitForSave, true,originOrder.getId());
        }
        return errors;
    }

    @Override
    public List<PclOrderB2cVO> queryList(Integer shipperId) {
        return pclOrderB2cDao.selectByShipper(shipperId);
    }

    @Override
    public List<PclOrderB2cDetailItemVO> queryProductById(Long id) {
        List<PclOrderB2cDetailItemVO> productDetailList = pclBatchDao.findSkuBatchInfoByOrderId(id);
        productDetailList.forEach(pclOrderB2cDetailItemVO -> {
            Short shippingBagPackedCode = pclOrderB2cDetailItemVO.getShippingBagPacked();
            if (shippingBagPackedCode != null) {
                ShippingBagPacked shippingBagPacked = ShippingBagPacked.get(shippingBagPackedCode);
                if (shippingBagPacked != null) {
                    pclOrderB2cDetailItemVO.setShippingBagPackedMsg(shippingBagPacked.getMessage());
                }
            }
        });
        //查询订单已关联的退货单 需要减去当前订单已经退货的商品数量
        List<PclProductReturn> returnedProducts = pclOrderReturnDao.findReturnProductsByOrderId(id);
        if(Detect.notEmpty(returnedProducts)){
            Map<String, Long> returnProductMap = returnedProducts.stream().collect(Collectors.groupingBy(PclProductReturn::getSku, Collectors.summingLong(PclProductReturn::getProductCount)));
            productDetailList.forEach(product ->{
                Long returnedCount = returnProductMap.get(product.getSku());
                if(returnedCount != null){
                    Long productTotalCount = product.getProductCount();
                    productTotalCount = productTotalCount != null ? productTotalCount : 0L;
                    product.setProductCount(productTotalCount - returnedCount);
                }
            });
        }
        return productDetailList;
    }

    @Override
    public SysOverseasWarehouse queryOverseasWarehouseById(Long id) {
        return pclOrderB2cDao.findOverseasWarehouseByOrderId(id);
    }

    @Transactional(rollbackFor = Exception.class)
    public void doSaveSplitOrder(List<Long> productIdList, List<B2cOrderSplitVO> waitForSave, boolean isReallySave,Long originOrderId) {
        //删除原始订单关联商品
        pclProductB2cDao.deleteByIdList(productIdList);
        StringBuilder operationLogContent=new StringBuilder();
        operationLogContent.append("拆分订单：");
        int gap = 0;
        for (int i = 0; i < waitForSave.size(); i++) {
            B2cOrderSplitVO b2cOrderSplitVO = waitForSave.get(i);
            final PclOrderB2c orderB2c = b2cOrderSplitVO.getOrderB2c();
            String oldReferenceNo = orderB2c.getReferenceNo();
            String newReferenceNo;
            boolean isDuplicate;
            do {
                newReferenceNo = oldReferenceNo + '-' + (i + 1 + gap);
                isDuplicate = checkRefNoDuplicate(newReferenceNo, orderB2c.getShipperId());
                if(isDuplicate){
                    //重复 后缀序号往后加1
                    gap++;
                    log.info("订单参考号:{}已存在,拆单参考号后缀将以此往后递增", newReferenceNo);
                }
            } while (isDuplicate);
            orderB2c.setReferenceNo(newReferenceNo);
            //异常原因置空
            orderB2c.setAbnormalCauses(null);
            final List<PclProductB2c> productB2cs = b2cOrderSplitVO.getProductB2cs();
            List<PclProductB2cDTO> pclProductB2cDTOs = productB2cs.stream().map(item ->{
                PclProductB2cDTO productB2cDTO = new PclProductB2cDTO();
                productB2cDTO.setSku(item.getSku());
                productB2cDTO.setProductCount(item.getProductCount() != null ? item.getProductCount().intValue() : 0);
                return productB2cDTO;
            }).collect(Collectors.toList());
            OrderPickingType pickingType = getPickingType(pclProductB2cDTOs);
            orderB2c.setPickingType(pickingType != null ? pickingType.getType() : null);
            if (orderB2c.getId() == null) {
                orderB2c.setDateCreated(null);
                orderB2c.setDateModified(null);
                orderB2c.setSource(OrderSource.SPLIT_ORDER.getType());
                orderB2c.initCreateEntity();
                pclOrderB2cDao.insert(orderB2c);
                List<TrkSourceOrderEventDTO> events=new ArrayList<>();
                orderEventService.packageEvents(orderB2c.getOrderNo(),orderB2c.getReferenceNo(),orderB2c.getId(),events, WareHouseOrderEventCode.ORDER_CREATED);
                //创建事件
                orderEventService.addEventsByApi(events);
            } else {
                orderB2c.initUpdateEntity();
                pclOrderB2cDao.updateByPrimaryKeySelective(orderB2c);
            }
            operationLogContent.append(orderB2c.getReferenceNo()).append("：");
            for (PclProductB2c productB2c : productB2cs) {
                if (productB2c.getId() == null) {
                    productB2c.setDateCreated(null);
                    productB2c.setDateModified(null);
                    productB2c.initCreateEntity();
                    productB2c.setOrderId(orderB2c.getId());
                    pclProductB2cDao.insert(productB2c);
                } else {
                    productB2c.initUpdateEntity();
                    pclProductB2cDao.updateByPrimaryKeySelective(productB2c);
                }
                operationLogContent.append(productB2c.getSku()).append("*").append(productB2c.getProductCount()).append(";");
            }
        }
        //记录拆单log
        createSplitOrderOperatorLog(originOrderId,operationLogContent);
        if (!isReallySave) {
            throw new RuntimeException("not save for test");
        }
    }

    @Override
    public Boolean cancelBefore(Long id) {
        //校验订单是否对接了海外仓取消接口
        PclOrderB2cDTO pclOrderB2cDTO = pclOrderB2cDao.selectOrderInfoById(id);
        Integer platform = pclOrderB2cDTO.getWarehousePlatform();
        if(platform!=null){
            return true;
        }
        return false;
    }


    private boolean checkRefNoDuplicate(String newReferenceNo, Integer shipperId) {
        //校验referenceNo重复
        List<PclOrderB2c> repeatList = pclOrderB2cDao.findByReferenceNo(newReferenceNo, shipperId);
        if (Detect.notEmpty(repeatList)) {
            return true;
        }
        return false;
    }

    private void createSplitOrderOperatorLog(Long originOrderId, StringBuilder operationLogContent) {
        SysOperationLog sysOperationLog=new SysOperationLog();
        sysOperationLog.setMethod("b2c/order/split");
        sysOperationLog.setOperationDescription("海外仓订单拆分");
        sysOperationLog.setDataId(originOrderId);
        sysOperationLog.setChangeContent(operationLogContent.toString());
        sysOperationLogService.addOperationLog(sysOperationLog);
    }

    @Resource
    private NumberGenerateService numberGenerateService;

    private B2cOrderSplitVO addSplitOrder(int currentIndex, PclOrderB2c order,
                                          B2cOrderSplitRequest req, Map<Long, PclProductB2c> productB2cMap, PartyVo shipperInfo) {
        B2cOrderSplitVO b2cOrderSplitVO = new B2cOrderSplitVO();
        final PclOrderB2c newOrder = BeanUtils.transform(order, PclOrderB2c.class);
//        String newReferenceNo = order.getReferenceNo() + '-' + (currentIndex + 1);
//        newOrder.setReferenceNo(newReferenceNo);
        newOrder.setStatus(B2cOrderStatus.UNCONFIRMED.getType());
        fillOriginOrderNo(order, newOrder);
        //先都设置为id为空 后面过滤了商品为空的订单后重新覆盖第一个订单为原订单
        newOrder.setId(null);
//            String orderNo = numberGenerateService.generateOrderNo(SessionContext.getContext().getTenantId());
        String orderNo = BusinessNo.generator().partyId(shipperInfo.getTenantId()).businessNoType(BusinessNoType.B2C_ORDER_NO).generate();
        newOrder.setOrderNo(orderNo);

        List<PclProductB2c> productB2cs = new ArrayList<>();
        for (B2cOrderSplitRequest.B2cOrderSplitRequestItem item : req.getItems()) {
            if (item.getRemainProductQty() != null && item.getRemainProductQty() > 0) {
                b2cOrderSplitVO.setHaveRemain(true);
            }
            final Long newCount = item.getDetails().get(currentIndex);
            //拆分订单后 过滤掉子订单中商品数量为0的商品
            if(newCount == 0){
                continue;
            }
            final Long productId = item.getProductId();
            final PclProductB2c originProduct = productB2cMap.get(productId);
            PclProductB2c newProduct = BeanUtils.transform(originProduct, PclProductB2c.class);
            newProduct.setProductCount(newCount);
            //每次拆分订单都删除原始订单 再插入拆分订单
            newProduct.setId(null);
            productB2cs.add(newProduct);
        }
        b2cOrderSplitVO.setIndex(currentIndex);
        b2cOrderSplitVO.setOrderB2c(newOrder);
        b2cOrderSplitVO.setProductB2cs(productB2cs);
        return b2cOrderSplitVO;
    }

    private void fillOriginOrderNo(PclOrderB2c order, PclOrderB2c newOrder) {
        //保存最初原始订单号到原始订单号字段
        if(!Detect.notEmpty(order.getOriginOrderNo())){
            //当前被拆分订单 首次被拆分
            newOrder.setOriginOrderNo(order.getOrderNo());
        }else{
            //当前被拆分订单 非首次被拆分
            newOrder.setOriginOrderNo(order.getOriginOrderNo());
        }
        //保存最初原始参考号到原始参考号字段
        if(!Detect.notEmpty(order.getOriginReferenceNo())){
            //当前被拆分订单 首次被拆分
            newOrder.setOriginReferenceNo(order.getReferenceNo());
        }else{
            //当前被拆分订单 非首次被拆分
            newOrder.setOriginReferenceNo(order.getOriginReferenceNo());
        }
    }

    private B2cOrderSplitVO addRemainOrder(PclOrderB2c order, Map<Long, PclProductB2c> productB2cMap,
                                           B2cOrderSplitRequest req, int orderIndex, PartyVo shipperInfo) {
        B2cOrderSplitVO vo = new B2cOrderSplitVO();
        final PclOrderB2c newOrder = BeanUtils.transform(order, PclOrderB2c.class);
//        newOrder.setReferenceNo(order.getReferenceNo() + '-' + (orderIndex));
        newOrder.setId(null);
        String orderNo = BusinessNo.generator().partyId(shipperInfo.getTenantId()).businessNoType(BusinessNoType.B2C_ORDER_NO).generate();
        newOrder.setOrderNo(orderNo);
        newOrder.setStatus(B2cOrderStatus.UNCONFIRMED.getType());
        fillOriginOrderNo(order, newOrder);
        List<PclProductB2c> productB2cs = new ArrayList<>();

        for (B2cOrderSplitRequest.B2cOrderSplitRequestItem item : req.getItems()) {
            final Long remainCount = item.getRemainProductQty();
            if (remainCount != null && remainCount > 0) {
                final PclProductB2c originProduct = productB2cMap.get(item.getProductId());
                PclProductB2c newProduct = BeanUtils.transform(originProduct, PclProductB2c.class);
                newProduct.setProductCount(remainCount);
                newProduct.setId(null);
                productB2cs.add(newProduct);
            }
        }

        vo.setOrderB2c(newOrder);
        vo.setProductB2cs(productB2cs);
        return vo;
    }

    private Map<Long, PclProductB2c> getProductMap(Long orderId) {
        PclProductB2c b2c = new PclProductB2c();
        b2c.setOrderId(orderId);
        final List<PclProductB2c> list = pclProductB2cDao.select(b2c);
        if (list == null || list.size() == 0) {
            log.info("拆分前订单商品为空");
            return Collections.emptyMap();
        }
        log.info("拆分前订单商品:{}", JSON.toJSONString(list));
        Map<Long, PclProductB2c> map = new HashMap<>();
        for (PclProductB2c pclProductB2c : list) {
            map.put(pclProductB2c.getId(), pclProductB2c);
        }
        return map;
    }

    private PclOrderB2c getOrderById(Long orderId) {
        PclOrderB2c b2c = new PclOrderB2c();
        b2c.setId(orderId);
        return pclOrderB2cDao.selectLimitOne(b2c);
    }

    private void editB2cOrderByDetailVO(PclOrderB2cDetailVO pclOrderB2cDetailVO, List<SubError> errorList) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        Long id = pclOrderB2cDetailVO.getId();
        PclOrderB2c pclOrderB2c = pclOrderB2cDao.selectByPrimaryKey(id);
        SysOverseasWarehouse oldWarehouse = sysOverseasWarehouseDao.queryByCode(pclOrderB2c.getShipperWarehouseCode(), aggregator);
        Short status = pclOrderB2c.getStatus();
        // 已创建，下单异常，出库异常（易仓）可修改
        boolean outboundAbnormalAllowEdit = isOutboundAbnormalAllowEdit(pclOrderB2c);
        if (status != null && !(B2cOrderStatus.UNCONFIRMED.getType().equals(status)
                || B2cOrderStatus.SENDING_ABNORMALITY.getType().equals(status)
                || outboundAbnormalAllowEdit)) {
            errorList.add(SubError.build(PclOrderB2cResultCode.B2C_ORDER_STATUS_CANNOT_EDIT));
            return;
        }
        if(outboundAbnormalAllowEdit){
            //修改时需要进行元数据校验 包裹信息
            //元数据校验 包裹尺寸
            List<PclOrderB2cDetailItemVO> pclProductList = pclProductB2cDao.findDetailInfoByOrderId(pclOrderB2c.getId());
            List<String> thirdSkuNoList = pclProductList.stream().map(PclOrderB2cDetailItemVO::getThirdSkuNo).collect(Collectors.toList());
            List<SysProduct> sysProductList = sysProductDao.findByThirdSkuAndShipperList(thirdSkuNoList,pclOrderB2c.getShipperId());
            List<SysProductCheckDTO> sysCheckProductList = new ArrayList<>();
            if(CollectionUtils.isNotEmpty(sysProductList)){
                Map<String, Long> skuMap =pclProductList.stream()
                        .collect(Collectors.toMap(PclOrderB2cDetailItemVO::getSku, PclOrderB2cDetailItemVO::getProductCount, Long::sum));
                sysProductList.forEach(e ->{
                    e.setGoodsQty(skuMap.get(e.getSku()).intValue());
                });
                sysCheckProductList = BeanUtils.transform(sysProductList, SysProductCheckDTO.class);
            }
            List<SysAddressRuleConfig> addressRuleConfigList = sysAddressRuleConfigDao.getActiveByChannelId(pclOrderB2c.getChannelId());
            List<SubError> validateUploadPacketWeightError = sysAddressRuleConfigService.validateUploadPacketWeight(sysCheckProductList, addressRuleConfigList);
            if(CollectionUtils.isNotEmpty(validateUploadPacketWeightError)){
                errorList.addAll(validateUploadPacketWeightError);
                return;
            }
        }
        BeanUtils.transform(pclOrderB2cDetailVO, pclOrderB2c);
        //处理附件
        List<Long> deleteFileIdList = pclOrderB2cDetailVO.getDeleteFileIdList();
        if(Detect.notEmpty(deleteFileIdList)){
            pclOrderB2cAttachmentDao.deleteByIdList(deleteFileIdList);
        }
        //保存文件列表
        List<PclOrderB2cAttachment> fileList = pclOrderB2cDetailVO.getFileList();
        if (Detect.notEmpty(fileList)) {
            SysOverseasWarehouse newWarehouse = sysOverseasWarehouseDao.queryByCode(pclOrderB2cDetailVO.getShipperWarehouseCode(), aggregator);
            WmsHandleStrategyService wmsHandleStrategyService = wmsHandleServiceMap.get(newWarehouse.getPlatForm());
            if(wmsHandleStrategyService==null){
                fileList.forEach(file->{
                    if(file.getId()==null){
                        file.initCreateEntity();
                        file.setOrderId(pclOrderB2cDetailVO.getId());
                        pclOrderB2cAttachmentDao.insertSelective(file);
                    }
                });
                log.error("Method [{}],platform id [{}] can not match any handle service, please check...", "upload B2c Order File", newWarehouse.getPlatForm());
            }else{
                //同平台下不需要重新上传附件，不同平台需要重新上传
                if(oldWarehouse.getPlatForm()==null||!oldWarehouse.getPlatForm().equals(newWarehouse.getPlatForm())){
                    //将文件发送到海外仓系统
                    for(PclOrderB2cAttachment file:fileList){
                        String fileType = file.getFileName().substring(file.getFileName().lastIndexOf(".")+1);
                        WmsUploadFileRequestDTO wmsUploadFileRequestDTO=new WmsUploadFileRequestDTO();
                        wmsUploadFileRequestDTO.setFileData(AWSFileUtils.getBase64ByFileId(file.getFileId()));
                        wmsUploadFileRequestDTO.setFileType(fileType);
                        wmsUploadFileRequestDTO.setModule("order_attach");
                        if (Objects.equals(file.getFileType(), OrderAttachmentType.LABEL.getType())) {
                            wmsUploadFileRequestDTO.setModule("order_label");
                        }
                        WmsUploadFileResponseDTO wmsUploadFileResponseDTO = wmsHandleStrategyService.uploadFile(wmsUploadFileRequestDTO);
                        if(wmsUploadFileResponseDTO!=null){
                            if("Failure".equals(wmsUploadFileResponseDTO.getAsk())){
                                errorList.add(SubError.build(SystemResultCode.OPERATION_ERROR,wmsUploadFileResponseDTO.getErrMessage()));
                                break;
                            }else{
                                file.setThirdFileId(wmsUploadFileResponseDTO.getAttachId());
                                file.setThirdFileUrl(wmsUploadFileResponseDTO.getUrl());
                                if(file.getId()==null){
                                    file.setOrderId(pclOrderB2cDetailVO.getId());
                                    pclOrderB2cAttachmentDao.insertSelective(file);
                                }else{
                                    pclOrderB2cAttachmentDao.updateByPrimaryKeySelective(file);
                                }
                            }
                        }
                    }
                }else{
                    //将文件发送到海外仓系统
                    for(PclOrderB2cAttachment file:fileList){
                        if(!Detect.notEmpty(file.getThirdFileId())){
                            String fileType = file.getFileName().substring(file.getFileName().lastIndexOf(".")+1);
                            WmsUploadFileRequestDTO wmsUploadFileRequestDTO=new WmsUploadFileRequestDTO();
                            wmsUploadFileRequestDTO.setFileData(AWSFileUtils.getBase64ByFileId(file.getFileId()));
                            wmsUploadFileRequestDTO.setFileType(fileType);
                            wmsUploadFileRequestDTO.setModule("order_attach");
                            if (Objects.equals(file.getFileType(), OrderAttachmentType.LABEL.getType())) {
                                wmsUploadFileRequestDTO.setModule("order_label");
                            }
                            WmsUploadFileResponseDTO wmsUploadFileResponseDTO = wmsHandleStrategyService.uploadFile(wmsUploadFileRequestDTO);
                            if(wmsUploadFileResponseDTO!=null){
                                if("Failure".equals(wmsUploadFileResponseDTO.getAsk())){
                                    errorList.add(SubError.build(SystemResultCode.OPERATION_ERROR,wmsUploadFileResponseDTO.getErrMessage()));
                                    break;
                                }else{
                                    file.setThirdFileId(wmsUploadFileResponseDTO.getAttachId());
                                    file.setThirdFileUrl(wmsUploadFileResponseDTO.getUrl());
                                    if(file.getId()==null){
                                        file.setOrderId(pclOrderB2cDetailVO.getId());
                                        pclOrderB2cAttachmentDao.insertSelective(file);
                                    }else{
                                        pclOrderB2cAttachmentDao.updateByPrimaryKeySelective(file);
                                    }
                                }
                            }else{
                                if(file.getId()==null){
                                    file.initCreateEntity();
                                    file.setOrderId(pclOrderB2cDetailVO.getId());
                                    pclOrderB2cAttachmentDao.insertSelective(file);
                                }
                            }
                        }
                    }
                }
                if(Detect.notEmpty(errorList)){
                    return ;
                }
            }
        }
        PartyVo shipperInfo = sysPartyService.findByPartyId(pclOrderB2cDetailVO.getShipperId());
        List<PclOrderB2cDetailItemVO> pclOrderB2cDetailItemList = pclOrderB2cDetailVO.getPclOrderB2cDetailItemList();
        //计算总费用
        BigDecimal totalValue = pclOrderB2cDetailItemList.stream().map(product -> {
            if (product.getUnitValue() == null) {
                return BigDecimal.ZERO;
            } else {
                if (product.getProductCount() != null && product.getProductCount() != 0) {
                    return product.getUnitValue().multiply(new BigDecimal(product.getProductCount().toString()));
                } else {
                    return BigDecimal.ZERO;
                }
            }
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
        pclOrderB2c.setInvoiceValue(totalValue);
        pclOrderB2c.setInvoiceCurrency(pclOrderB2cDetailItemList.get(0).getUnitValueCurrency());
        //创建地址信息
        SysAddress shipperAddress = pclOrderB2cDetailVO.getShipperAddress();
        String shipperAddressId = createAddress(shipperAddress);
        pclOrderB2c.setShipperAddressId(shipperAddressId);
        SysAddress recipientAddress = pclOrderB2cDetailVO.getRecipientAddress();
        String recipientAddressId = createAddress(recipientAddress);
        pclOrderB2c.setRecipientAddressId(recipientAddressId);
        //插入shipper信息
        pclOrderB2c.setShipperName(shipperInfo.getNameEn());
        pclOrderB2c.initUpdateEntity();
        //设置拣货类型
        List<PclProductB2cDTO> pclProductB2cDTOs = pclOrderB2cDetailItemList.stream().map(item ->{
            PclProductB2cDTO productB2cDTO = new PclProductB2cDTO();
            productB2cDTO.setSku(item.getSku());
            productB2cDTO.setProductCount(item.getProductCount() != null ? item.getProductCount().intValue() : 0);
            return productB2cDTO;
        }).collect(Collectors.toList());
        OrderPickingType pickingType = getPickingType(pclProductB2cDTOs);
        pclOrderB2c.setPickingType(pickingType != null ? pickingType.getType() : null);
        pclOrderB2cDao.updateByPrimaryKeySelective(pclOrderB2c);
        //记录服务校验切换日志
        if(pclOrderB2c.getServiceChanged()!=null&&pclOrderB2c.getServiceChanged()){
            recordValidationAddressLog(pclOrderB2c,pclOrderB2cDetailVO,"Edit Order");
        }
        //删除需要删除的商品信息
        if (Detect.notEmpty(pclOrderB2cDetailVO.getDeleteItemId())) {
            pclProductB2cDao.deleteByIdList(pclOrderB2cDetailVO.getDeleteItemId());
        }
        //添加货修改订单的商品信息
        if (Detect.notEmpty(pclOrderB2cDetailItemList)) {
            List<PclProductB2c> addProductList = new ArrayList<>();
            pclOrderB2cDetailItemList.forEach(pclOrderB2cDetailItemVO -> {
                PclProductB2c pclProductB2c = BeanUtils.transform(pclOrderB2cDetailItemVO, PclProductB2c.class);
                if (pclOrderB2cDetailItemVO.getProductId() == null) {
                    pclProductB2c.initCreateEntity();
                    pclProductB2c.setShipperId(pclOrderB2cDetailVO.getShipperId());
                    pclProductB2c.setOrderId(pclOrderB2cDetailVO.getId());
                    pclProductB2c.setOverseasWarehouseCode(pclOrderB2cDetailVO.getShipperWarehouseCode());
                    addProductList.add(pclProductB2c);
                } else {
                    //修改商品信息
                    pclProductB2c.initUpdateEntity();
                    pclProductB2c.setId(pclOrderB2cDetailItemVO.getProductId());
                    pclProductB2c.setShipperId(pclOrderB2cDetailVO.getShipperId());
                    pclProductB2c.setOverseasWarehouseCode(pclOrderB2cDetailVO.getShipperWarehouseCode());
                    pclProductB2cDao.updateByPrimaryKeySelective(pclProductB2c);
                }
            });
            if (Detect.notEmpty(addProductList)) {
                pclProductB2cDao.insertList(addProductList);
            }
        }
        //更新海外仓订单
        if (outboundAbnormalAllowEdit) {
            log.info("将同步更新第三方海外仓订单,客户订单号:{}...", pclOrderB2c.getReferenceNo());
            editThirdWarehouseOrder(pclOrderB2c, errorList);
        }
    }

    private void saveLabelFiles(List<PclOrderB2cAttachment> labelFileList, Long orderId) {
        labelFileList.forEach(file -> {
            if (file.getId() == null) {
                file.initCreateEntity();
                file.setOrderId(orderId);
                pclOrderB2cAttachmentDao.insertSelective(file);
            }
        });
    }

    private boolean editThirdWarehouseOrder(PclOrderB2c pclOrderB2c, List<SubError> errors) {
        String orderNo = pclOrderB2c.getOrderNo();
        List<WmsCreateOrderDetailDTO> orderProductDetailDTOList = pclOrderB2cDao.selectOrderAndProductInfoForWmsOrder(Arrays.asList(orderNo));
        if (!Detect.notEmpty(orderProductDetailDTOList)) {
            return false;
        }
        //组装成统一结构
        WmsCreateOrderDetailDTO orderDetailDTO = orderProductDetailDTOList.get(0);
        Integer platform = orderDetailDTO.getWarehousePlatform();
        WmsCreateUpdateOrderDTO wmsUpdateOrderDTO = BeanUtils.transform(orderDetailDTO, WmsCreateUpdateOrderDTO.class);
        wmsUpdateOrderDTO.setShipperWarehouseCode(Detect.notEmpty(orderDetailDTO.getThirdWarehouseCode()) ? orderDetailDTO.getThirdWarehouseCode() : orderDetailDTO.getShipperWarehouseCode());
        List<WmsCreateUpdateOrderItemDTO> orderItems = orderProductDetailDTOList.stream().map(orderDetail -> {
            WmsCreateUpdateOrderItemDTO orderItemDTO = BeanUtils.transform(orderDetail, WmsCreateUpdateOrderItemDTO.class);
            if(!(platform.equals(OverseasWarehousePlatform.MAERSK.getPlatformCode()) || platform.equals(OverseasWarehousePlatform.WMS_UBAY.getPlatformCode()))){
                orderItemDTO.setSku(orderDetail.getThirdSkuNo());
            }else{
                orderItemDTO.setSku(orderDetail.getSku());
            }
            return orderItemDTO;
        }).collect(Collectors.toList());
        wmsUpdateOrderDTO.setItems(orderItems);
        //调用海外仓修改接口
        WmsHandleStrategyService wmsStrategyService = wmsHandleServiceMap.get(platform);
        WmsCommonRespDTO wmsResponseDTO = wmsStrategyService.updateOrder(wmsUpdateOrderDTO);
        Boolean success = wmsResponseDTO.getSuccess();
        PclOrderB2c updatePclOrderB2c = new PclOrderB2c();
        updatePclOrderB2c.setId(pclOrderB2c.getId());
        if (success) {
            //海外仓更新成功 订单状态变更为 “已接收” 提交时间更新
            updatePclOrderB2c.setDateSending(new Date());
            updatePclOrderB2c.setStatus(B2cOrderStatus.CONFIRMED_OUTBOUND.getType());
            //先撤销历史账单
            log.info("订单号:{}, 先撤销历史账单", orderNo);
            warehouseCalculateFeeService.reverseFee(OverseasWarehouseCostType.HANDLE, orderNo, "修改订单，冲减原始费用");
            //再重新计算 订单处理费
            log.info("订单号:{}, 再重新计算订单处理费", orderNo);
            calculatorHandleFee(Arrays.asList(pclOrderB2c.getOrderNo()), true);
        } else {
            //海外仓更新失败 订单状态不变 记录异常原因
            String errorMsg = wmsResponseDTO.getErrorMsg();
            updatePclOrderB2c.setAbnormalCauses(com.walltech.common.utils.StringUtils.subStringNull(errorMsg, 1000));
            errors.add(SubError.build(PclOrderB2cResultCode.ORDER_EDIT_THIRD_WAREHOUSE_FAILED, errorMsg));
        }
        pclOrderB2cDao.updateByPrimaryKeySelective(updatePclOrderB2c);
        return success;
    }

    /**
     * 是否是出库异常且能编辑海外仓订单
     * 暂时只有易仓能在出库异常状态更新订单并同步到海外仓
     * @param pclOrderB2c
     * @return
     */
    private boolean isOutboundAbnormalAllowEdit(PclOrderB2c pclOrderB2c) {
        Short status = pclOrderB2c.getStatus();
        boolean isOutBoundAbnormal = ObjectUtils.equals(B2cOrderStatus.OUTBOUND_ABNORMALITY.getType(), status);
        String shipperWarehouseCode = pclOrderB2c.getShipperWarehouseCode();
        SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(shipperWarehouseCode, SessionContext.getContext().getAggregator());
        if (sysOverseasWarehouse == null) {
            return false;
        }
        Integer platform = sysOverseasWarehouse.getPlatForm();
        if (isOutBoundAbnormal && OverseasWarehousePlatform.getplatformCodeListByPlatform("ECCANG").contains(platform)) {
            return true;
        }
        return false;
    }

    private void createB2cOrderProduct(PclOrderB2c pclOrderB2c, PclOrderB2cDetailVO pclOrderB2cDetailVO) {
        List<PclOrderB2cDetailItemVO> pclOrderB2cDetailItemList = pclOrderB2cDetailVO.getPclOrderB2cDetailItemList();
        List<PclProductB2c> productB2cList = BeanUtils.transform(pclOrderB2cDetailItemList, PclProductB2c.class);
        productB2cList.forEach(pclProductB2c -> {
            pclProductB2c.initCreateEntity();
            pclProductB2c.setShipperId(pclOrderB2c.getShipperId());
            pclProductB2c.setOrderId(pclOrderB2c.getId());
            pclProductB2c.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
        });
        pclProductB2cDao.insertList(productB2cList);
    }

    private PclOrderB2c createB2cOrderByDetailVO(PclOrderB2cDetailVO pclOrderB2cDetailVO,List<SubError>errorList) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        PartyVo shipperInfo = sysPartyService.findByPartyId(pclOrderB2cDetailVO.getShipperId());
        PclOrderB2c pclOrderB2c = BeanUtils.transform(pclOrderB2cDetailVO, PclOrderB2c.class);
        pclOrderB2c.initCreateEntity();
        pclOrderB2c.setOrderNo((BusinessNo.generator().partyId(shipperInfo.getTenantId()).businessNoType(BusinessNoType.B2C_ORDER_NO).generate()));
        pclOrderB2c.setStatus(B2cOrderStatus.UNCONFIRMED.getType());
        List<PclOrderB2cDetailItemVO> pclOrderB2cDetailItemList = pclOrderB2cDetailVO.getPclOrderB2cDetailItemList();
        //计算总费用
        BigDecimal totalValue = pclOrderB2cDetailItemList.stream().map(product -> {
            if (product.getUnitValue() == null) {
                return BigDecimal.ZERO;
            } else {
                return product.getUnitValue().multiply(new BigDecimal(product.getProductCount().toString()));
            }
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
        pclOrderB2c.setInvoiceValue(totalValue);
        pclOrderB2c.setInvoiceCurrency(pclOrderB2cDetailItemList.get(0).getUnitValueCurrency());
        //保存文件列表
        List<PclOrderB2cAttachment> fileList = pclOrderB2cDetailVO.getFileList();
        if (Detect.notEmpty(fileList)) {
            SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(pclOrderB2cDetailVO.getShipperWarehouseCode(), aggregator);
            //将文件发送到海外仓系统
            WmsHandleStrategyService wmsHandleStrategyService = wmsHandleServiceMap.get(sysOverseasWarehouse.getPlatForm());
            if(wmsHandleStrategyService==null){
                log.error("Method [{}],platform id [{}] can not match any handle service, please check...", "upload B2c Order File", sysOverseasWarehouse.getPlatForm());
            }
            for(PclOrderB2cAttachment file:fileList){
                file.setOrderId(pclOrderB2c.getId());
                if (wmsHandleStrategyService != null) {
                    String fileType = file.getFileName().substring(file.getFileName().lastIndexOf(".")+1);
                    if(!Detect.notEmpty(file.getThirdFileId())){
                        WmsUploadFileRequestDTO wmsUploadFileRequestDTO=new WmsUploadFileRequestDTO();
                        wmsUploadFileRequestDTO.setFileData(AWSFileUtils.getBase64ByFileId(file.getFileId()));
                        wmsUploadFileRequestDTO.setFileType(fileType);
                        wmsUploadFileRequestDTO.setModule("order_attach");
                        if (Objects.equals(file.getFileType(), OrderAttachmentType.LABEL.getType())) {
                            wmsUploadFileRequestDTO.setModule("order_label");
                        }
                        WmsUploadFileResponseDTO wmsUploadFileResponseDTO = wmsHandleStrategyService.uploadFile(wmsUploadFileRequestDTO);
                        if(wmsUploadFileResponseDTO!=null){
                            if("Failure".equals(wmsUploadFileResponseDTO.getAsk())){
                                errorList.add(SubError.build(SystemResultCode.OPERATION_ERROR,wmsUploadFileResponseDTO.getErrMessage()));
                                break;
                            }else{
                                file.setThirdFileId(wmsUploadFileResponseDTO.getAttachId());
                                file.setThirdFileUrl(wmsUploadFileResponseDTO.getUrl());
                            }
                        }
                    }
                }
            }
            if(Detect.notEmpty(errorList)){
                return pclOrderB2c;
            }
            pclOrderB2cAttachmentDao.insertList(fileList);
        }
        //创建地址信息
        SysAddress shipperAddress = pclOrderB2cDetailVO.getShipperAddress();
        String shipperAddressId = createAddress(shipperAddress);
        pclOrderB2c.setShipperAddressId(shipperAddressId);
        SysAddress recipientAddress = pclOrderB2cDetailVO.getRecipientAddress();
        String recipientAddressId = createAddress(recipientAddress);
        pclOrderB2c.setRecipientAddressId(recipientAddressId);
        //插入shipper信息
        pclOrderB2c.setShipperName(shipperInfo.getNameEn());
        //插入order信息
        pclOrderB2c.setSource(OrderSource.MANUAL.getType());
        List<PclProductB2cDTO> pclProductB2cDTOs = pclOrderB2cDetailItemList.stream().map(item ->{
            PclProductB2cDTO productB2cDTO = new PclProductB2cDTO();
            productB2cDTO.setSku(item.getSku());
            productB2cDTO.setProductCount(item.getProductCount() != null ? item.getProductCount().intValue() : 0);
            return productB2cDTO;
        }).collect(Collectors.toList());
        OrderPickingType pickingType = getPickingType(pclProductB2cDTOs);
        //设置拣货类型(单品单件:一个sku总数量1;单品多件:一个sku总数量大于1;多品多件:多个sku)
        pclOrderB2c.setPickingType(pickingType != null ?pickingType.getType() : null);
        pclOrderB2cDao.insertSelective(pclOrderB2c);
        //赋值id用于日志比较
        pclOrderB2cDetailVO.setId(pclOrderB2c.getId());
        //记录服务校验切换日志
        if(pclOrderB2c.getServiceChanged()!=null&&pclOrderB2c.getServiceChanged()){
            recordValidationAddressLog(pclOrderB2c,pclOrderB2cDetailVO,"Add Order");
        }
        return pclOrderB2c;
    }

    private void recordValidationAddressLog(PclOrderB2c pclOrderB2c,PclOrderB2cDetailVO pclOrderB2cDetailVO,String method) {
        SysOperationLog sysOperationLog=new SysOperationLog();
        sysOperationLog.setDataId(pclOrderB2c.getId());
        sysOperationLog.setMethod(method);
        sysOperationLog.setOperatorNameEn("Validation Address API");
        sysOperationLog.setOperatorName("Validation Address API");
        String changeContentEn = "Service:" + "(" + pclOrderB2cDetailVO.getChangeServiceBeforeCode() + ")" + pclOrderB2cDetailVO.getChangeServiceBeforeName() +
                "—>" + "(" + pclOrderB2cDetailVO.getServiceCode() + ")" + pclOrderB2cDetailVO.getServiceName();
        String changeContent = "选用服务:" + "(" + pclOrderB2cDetailVO.getChangeServiceBeforeCode() + ")" + pclOrderB2cDetailVO.getChangeServiceBeforeName() +
                "—>" + "(" + pclOrderB2cDetailVO.getServiceCode() + ")" + pclOrderB2cDetailVO.getServiceName();
        sysOperationLog.setChangeContent(changeContent);
        sysOperationLog.setChangeContentEn(changeContentEn);
        sysOperationLog.initCreateEntity();
        sysOperationLogDao.insertSelective(sysOperationLog);
    }

    @Override
    public OrderPickingType getPickingType(List<?> items) {
        if(!Detect.notEmpty(items)){
            return null;
        }
        List<PclProductB2cDTO> productB2cList = BeanUtils.transform(items, PclProductB2cDTO.class);
        //设置拣货类型(单品单件:一个sku总数量1;单品多件:一个sku总数量大于1;多品多件:多个sku)
        //由于不用接口使用的实体商品数量（productCount或itemCount）属性名不一致，兼容处理
        Map<String, Integer> skuMap = productB2cList.stream().collect(Collectors.groupingBy(p -> p.getSku(), Collectors.summingInt(p -> p.getProductCount() != null ? p.getProductCount() : 0)));
        OrderPickingType pickingType = null;
        if(skuMap.size() > 1){
            pickingType = OrderPickingType.MULTIPLE;
        }else if(skuMap.size() == 1){
            Integer productCount = skuMap.values().stream().reduce(0, Integer::sum);
            if(productCount == 1){
                pickingType = OrderPickingType.SINGLE;
            }else if(productCount > 1){
                pickingType = OrderPickingType.SINGLE_SKU_WITH_MULTIPLE_QTY;
            }
        }
        return pickingType;
    }

    private String createAddress(SysAddress sysAddress) {
        String uuid = AddressUtils.generateId(sysAddress);
        sysAddress.setUuid(uuid);
        boolean flag = sysAddressDao.existsWithPrimaryKey(uuid);
        if (!flag) {
            sysAddress.initAddressCreateEntity();
            sysAddressDao.insertSelective(sysAddress);
        }
        return sysAddress.getUuid();
    }

    private BigDecimal calculateProductFee(B2cProductInfo productInfo, List<ActHandleReturnDetail> rateDetails, ActOverseasWarehouseRate rate, SysOverseasWarehouseShipperRateDetailDTO lastMileBaseRateInfo, List<ActOverseasWarehouseBillingItemHandling> billingItemList) {
        BigDecimal weight = calculateProductBubbleWeight(productInfo, rate, lastMileBaseRateInfo);
        if (weight == null) {
            log.error("商品详情重量为空sku: {}", productInfo.getSku());
            return BigDecimal.ZERO;
        }
        ActHandleReturnDetail selectedDetail = null;
        for (ActHandleReturnDetail rateDetail : rateDetails) {
            BigDecimal weightStart = rateDetail.getWeightStart();
            BigDecimal weightEnd = rateDetail.getWeightEnd();
            if (weightEnd != null && BigDecimalUtils.gt(weight, weightEnd)) {
                continue;
            }
            if (weightStart != null && BigDecimalUtils.le(weight, weightStart)) {
                continue;
            }
            selectedDetail = rateDetail;
            break;
        }
        if (selectedDetail == null) {
            log.error("该商品重量没有找到合适的价卡sku: {}", productInfo.getSku());
            return BigDecimal.ZERO;
        }
        BigDecimal itemPrice = selectedDetail.getItemPrice();
        if (itemPrice == null) {
            log.error("没有设置计费单价价卡sku: {}", productInfo.getSku());
            return BigDecimal.ZERO;
        }
        Long productCount = productInfo.getProductCount();
        if (productCount == null) {
            log.warn("sku商品数量为null");
            productCount = 1L;
        }
        BigDecimal amount = itemPrice.multiply(new BigDecimal(productCount));
        //>>>>>记录商品计费明细 start>>>>>
        ActOverseasWarehouseBillingItemHandling billingItem = recordHandlingBillingItemInfo(productInfo);
        billingItem.setMatchingWeight(weight);
        billingItem.setBillingType(OverseasWarehouseRateBillingType.ITEM.getType());
        billingItem.setHitPrice(itemPrice);
        billingItem.setRateCardAmount(amount);
        billingItemList.add(billingItem);
        //>>>>>记录商品计费明细 end>>>>>
        return amount;
    }

    private BigDecimal calculateProductBubbleWeight(B2cProductInfo productInfo, ActOverseasWarehouseRate rate, SysOverseasWarehouseShipperRateDetailDTO lastMileBaseRateInfo) {
        BigDecimal resultWeight;
        BigDecimal weight = productInfo.getWeight();
        String orderNo = productInfo.getOrderNo();
        BigDecimal productLengthActual = BigDecimalUtils.covertNullToZero(productInfo.getProductLengthActual());
        BigDecimal productHeightActual = BigDecimalUtils.covertNullToZero(productInfo.getProductHeightActual());
        BigDecimal productWidthActual = BigDecimalUtils.covertNullToZero(productInfo.getProductWidthActual());
        BigDecimal volume = productLengthActual.multiply(productHeightActual).multiply(productWidthActual);
        BigDecimal heavyBulkyRatio = rate.getHeavyBulkyRatio();
        BigDecimal weightMultiple = rate.getWeightMultiple();
        Short bubbleRuleType = rate.getBubbleRuleType();
        Boolean conditionOneActive = rate.getConditionOneActive();
        Boolean conditionTwoActive = rate.getConditionTwoActive();
        BigDecimal lwhPlusThreshold = rate.getLwhPlusThreshold();
        //尾程价卡的泡重
        if (OverseasWarehouseRateBubbleRuleType.LAST_MILE_RATE.getType().equals(bubbleRuleType)) {
            //获取订单对应的尾程价卡
            Long lastMileRateId = lastMileBaseRateInfo.getRateId();
            if (lastMileRateId == null) {
                log.info("计算订单处理费泡重,海外仓{},未配置尾程价卡", lastMileBaseRateInfo.getWarehouseCode());
                return weight;
            } else {
                ActLastMileRate actLastMileRate = actLastMileRateDao.selectByPrimaryKey(lastMileRateId);
                if (actLastMileRate == null) {
                    log.info("计算订单处理费泡重,海外仓{},尾程价卡未找到", lastMileBaseRateInfo.getWarehouseCode());
                    return weight;
                } else {
                    resultWeight = calculateWeight(orderNo, weight, volume, actLastMileRate.getWeightMultiple(), actLastMileRate.getHeavyBulkyRatio(), actLastMileRate.getLwhPlusThreshold(), actLastMileRate.getConditionOneActive(), actLastMileRate.getConditionTwoActive());
                }
            }
        } else if (OverseasWarehouseRateBubbleRuleType.CUSTOMIZE.getType().equals(bubbleRuleType)) {
            //配置的泡重
            resultWeight = calculateWeight(orderNo, weight, volume, weightMultiple, heavyBulkyRatio, lwhPlusThreshold, conditionOneActive, conditionTwoActive);
        } else {
            return weight;
        }
        return resultWeight;
    }
    private BigDecimal calculateOrderBubbleWeight(String orderNo,BigDecimal totalWeight, BigDecimal totalVolume, ActOverseasWarehouseRate rate, SysOverseasWarehouseShipperRateDetailDTO lastMileBaseRateInfo) {
        BigDecimal resultWeight;
        BigDecimal heavyBulkyRatio = rate.getHeavyBulkyRatio();
        BigDecimal weightMultiple = rate.getWeightMultiple();
        Short bubbleRuleType = rate.getBubbleRuleType();
        Boolean conditionOneActive = rate.getConditionOneActive();
        Boolean conditionTwoActive = rate.getConditionTwoActive();
        BigDecimal lwhPlusThreshold = rate.getLwhPlusThreshold();
        //尾程价卡的泡重
        if (OverseasWarehouseRateBubbleRuleType.LAST_MILE_RATE.getType().equals(bubbleRuleType)) {
            //获取订单对应的尾程价卡
            Long lastMileRateId = lastMileBaseRateInfo.getRateId();
            if (lastMileRateId == null) {
                log.info("计算订单处理费泡重,海外仓{},未配置尾程价卡", lastMileBaseRateInfo.getWarehouseCode());
                return totalWeight;
            } else {
                ActLastMileRate actLastMileRate = actLastMileRateDao.selectByPrimaryKey(lastMileRateId);
                if (actLastMileRate == null) {
                    log.info("计算订单处理费泡重,海外仓{},尾程价卡未找到", lastMileBaseRateInfo.getWarehouseCode())  ;
                    return totalWeight;
                } else {
                    resultWeight = calculateWeight(orderNo, totalWeight, totalVolume, actLastMileRate.getWeightMultiple(), actLastMileRate.getHeavyBulkyRatio(), actLastMileRate.getLwhPlusThreshold(), actLastMileRate.getConditionOneActive(), actLastMileRate.getConditionTwoActive());
                }
            }
        } else if (OverseasWarehouseRateBubbleRuleType.CUSTOMIZE.getType().equals(bubbleRuleType)) {
            //配置的泡重
            resultWeight = calculateWeight(orderNo, totalWeight, totalVolume, weightMultiple, heavyBulkyRatio, lwhPlusThreshold, conditionOneActive, conditionTwoActive);
        } else {
            return totalWeight;
        }
        return resultWeight;
    }
    /**
     * 计算泡重规则后的重量
     *
     * @param weight               未计算泡重的重量
     * @param weightMultiple       重量比
     * @param heavyBulkyRatio      重泡比
     * @param lwhPlusThreshold     长宽高阀值
     * @param isActiveConditionOne 是否计算计算长宽高阀值 false则 不计算
     * @param isActiveConditionTwo 是否计算计算长宽高阀值 false则 不计算 lwhPlusThreshold
     * @return
     */
    private BigDecimal calculateWeight(String orderNo, BigDecimal weight, BigDecimal volume, BigDecimal weightMultiple, BigDecimal heavyBulkyRatio, BigDecimal lwhPlusThreshold, Boolean isActiveConditionOne, Boolean isActiveConditionTwo) {
        BigDecimal resultWeight;
        //false则 不计算 lwhPlusThreshold
        if (!isActiveConditionOne) {
            log.info("出库单:{},条件一为禁用状态,最终计费重量为实际重量", orderNo);
            return weight;
        } else {
            if (BigDecimalUtils.isZero(heavyBulkyRatio)) {
                log.info("出库单:{},条件一参数[重泡比]除数不能为0,将使用实际重量为计费重量", orderNo);
                return weight;
            }
            BigDecimal value1 = volume.divide(heavyBulkyRatio, 6, RoundingMode.HALF_UP);
            BigDecimal value2 = weightMultiple.multiply(weight);
            if (BigDecimalUtils.gt(value1, value2)) {
                resultWeight = value1;
            } else {
                resultWeight = weight;
            }
        }
        // todo 条件二计算逻辑待定
        return resultWeight;
    }

    private ActHandleReturnDetailDTO getRateDetail(Long handleId, Date now) {
        ActHandleReturnDetailDTO actHandleReturnDetailDTO = new ActHandleReturnDetailDTO();
        List<ActOverseasWarehouseItem> items = actStorageFeeRateItemDao.select(new ActOverseasWarehouseItem()
                .setRateId(handleId));
        if (!Detect.notEmpty(items)) {
            log.error("处理费: {} 没有item列表", handleId);
            return actHandleReturnDetailDTO;
        }
        ActOverseasWarehouseItem selectedItem = null;
        for (ActOverseasWarehouseItem item : items) {
            Date validityStart = item.getValidityStart();
            Date validityEnd = item.getValidityEnd();
            if (validityStart != null) {
                if (validityStart.after(now)) {
                    continue;
                }
            }
            if (validityEnd != null) {
                if (now.after(validityEnd)) {
                    continue;
                }
            }
            selectedItem = item;
            break;
        }
        if (selectedItem == null) {
            log.error("处理费: {} 没有在有效期内的item", handleId);
            return actHandleReturnDetailDTO;
        }
        actHandleReturnDetailDTO.setAdditionalWeightFee(selectedItem.getAdditionalWeightFee());
        actHandleReturnDetailDTO.setAdditionalItemFee(selectedItem.getAdditionalItemFee());
        List<ActHandleReturnDetail> actHandleReturnDetailList = actHandleReturnDao.select(new ActHandleReturnDetail().setRateItemId(selectedItem.getId()));
        actHandleReturnDetailDTO.setActHandleReturnDetails(actHandleReturnDetailList);
        return actHandleReturnDetailDTO;
    }

    /**
     * 根据订单编号,获取订单sku信息
     *
     * @param orderNos 订单编号
     * @return 价卡配置
     */
    private Map<String, List<B2cProductInfo>> getProductInfo(List<String> orderNos) {
        Map<String, List<B2cProductInfo>> map = new HashMap<>();
        if (Detect.notEmpty(orderNos)) {
            List<B2cProductInfo> products = pclProductB2cDao.getProductSkuCountInfo(orderNos);
            Map<String, BigDecimal> weightMap = new HashMap<>();
            for (B2cProductInfo product : products) {
                product.setOrderWeight(product.getWeight());
                if (StringUtils.isNotEmpty(product.getSku())) {
                    String key = product.getSku() + "_" + product.getShipperId();
                    if (weightMap.containsKey(key)) {
                        product.setWeight(weightMap.get(key));
                    } else {
                        BigDecimal weightActual = getWeightBySkuAndWeight(product.getShipperId(), product.getSku());
                        weightMap.put(key, weightActual);
                        product.setWeight(weightActual);
                    }
                }
                String orderNo = product.getOrderNo();
                if (map.containsKey(orderNo)) {
                    List<B2cProductInfo> b2cProductInfos = map.get(orderNo);
                    b2cProductInfos.add(product);
                } else {
                    List<B2cProductInfo> b2cProductInfos = new ArrayList<>();
                    b2cProductInfos.add(product);
                    map.put(orderNo, b2cProductInfos);
                }
            }
        }
        return map;
    }

    private BigDecimal getWeightBySkuAndWeight(Integer shipperId, String sku) {
        Example example = Example.builder(SysProduct.class)
                .where(Sqls.custom()
                        .andEqualTo("sku", sku)
                        .andEqualTo("shipperId", shipperId)
                ).build();
        List<SysProduct> sysProducts = sysProductDao.selectByExample(example);
        if (sysProducts != null && sysProducts.size() > 0) {
            return sysProducts.get(0).getGrossWeightActual();
        }
        return null;
    }

    /**
     * 根据订单信息 获取海外仓价卡配置
     */
    private Map<String, SysOverseasWarehouseShipperRateDetailDTO> getOverseasWarehouseHandleRateConfig(List<B2cOrderAndShipperDTO> b2cOrderList) {
        //<key, handleRateBean>
        Map<String , SysOverseasWarehouseShipperRateDetailDTO> handleRateCacheMap = Maps.newHashMap();
        //<orderNo, handleRateBean>
        Map<String, SysOverseasWarehouseShipperRateDetailDTO> finalRateMap = new HashMap<>();
        for (B2cOrderAndShipperDTO b2cOrder : b2cOrderList) {
            Integer shipperId = b2cOrder.getShipperId();
            String shipperWarehouseCode = b2cOrder.getShipperWarehouseCode();
            String key = shipperId + "-" + shipperWarehouseCode;
            SysOverseasWarehouseShipperRateDetailDTO handleRateDTO = null;
            if(handleRateCacheMap.containsKey(key)){
                handleRateDTO = handleRateCacheMap.get(key);
            }else{
                handleRateDTO = sysOverseasWarehouseShipperService.getNonLastMileRateConfigInfo(shipperId, shipperWarehouseCode, OverseasWarehouseRateType.HANDLE);
                handleRateCacheMap.put(key, handleRateDTO);
            }
            finalRateMap.put(b2cOrder.getOrderNo(), handleRateDTO);
        }
        return finalRateMap;
    }

    private Map<String, SysOverseasWarehouseShipperRateDetailDTO> getOverseasWarehouseLastMileBaseRateConfig(List<B2cOrderAndShipperDTO> b2cOrderList) {
        //<key, handleRateBean>
        Map<String , SysOverseasWarehouseShipperRateDetailDTO> lastMileRateCacheMap = Maps.newHashMap();
        //<orderNo, handleRateBean>
        Map<String, SysOverseasWarehouseShipperRateDetailDTO> finalRateMap = new HashMap<>();
        for (B2cOrderAndShipperDTO b2cOrder : b2cOrderList) {
            Integer shipperId = b2cOrder.getShipperId();
            String shipperWarehouseCode = b2cOrder.getShipperWarehouseCode();
            Long channelId = b2cOrder.getChannelId();
            String key = shipperId + "-" + shipperWarehouseCode + "-" + channelId;
            SysOverseasWarehouseShipperRateDetailDTO lastMileBaseRateDTO = null;
            if(lastMileRateCacheMap.containsKey(key)){
                lastMileBaseRateDTO = lastMileRateCacheMap.get(key);
            }else{
                Map<Short, List<SysOverseasWarehouseShipperRateDetailDTO>> lastMileRateConfigInfoList
                        = sysOverseasWarehouseShipperService.getLastMileRateConfigInfo(shipperId, shipperWarehouseCode, channelId, new Date());
                List<SysOverseasWarehouseShipperRateDetailDTO> lastMileBaseRateList = lastMileRateConfigInfoList.get(OverseasWarehouseRateType.LAST_MILE_BASE_FEE.getType().shortValue());
                lastMileBaseRateDTO = Detect.notEmpty(lastMileBaseRateList) ? lastMileBaseRateList.get(0) : null;
                lastMileRateCacheMap.put(key, lastMileBaseRateDTO);
            }
            finalRateMap.put(b2cOrder.getOrderNo(), lastMileBaseRateDTO);
        }
        return finalRateMap;
    }

    private void dealWithLabelBase64Info(List<String> labelContentList, HttpServletResponse response) {
        List<byte[]> bytesList = new ArrayList<>();
        try {
            labelContentList.forEach(labelContent -> {
                byte[] bytes = Base64.getDecoder().decode(labelContent);
                bytesList.add(bytes);
            });
            PdfUtils.mergeAndRotate(bytesList, response.getOutputStream(), PdfUtils.PageSize.PAGE_SIZE_10X15);
        } catch (IOException e) {
            log.error("printLabels", e);
            response.reset();
        }
    }

    private PclOrderB2cPrintLabelsThirdDTO getPrintLabelThirdInfo(PclOrderB2cPrintLabelsRequest printLabelsRequest) {
        PclOrderB2cPrintLabelsThirdDTO pclOrderB2cPrintLabelsThirdDTO = new PclOrderB2cPrintLabelsThirdDTO();
        pclOrderB2cPrintLabelsThirdDTO.setOrderIds(printLabelsRequest.getOrderNos());
        pclOrderB2cPrintLabelsThirdDTO.setMerged(false);
        pclOrderB2cPrintLabelsThirdDTO.setLabelFormat("PDF");
        Short labelTypeCode = printLabelsRequest.getLabelType();
        LabelType labelType = LabelType.getByType(labelTypeCode);
        if (labelType == null) {
            return null;
        }
        pclOrderB2cPrintLabelsThirdDTO.setLabelType(labelType.getTypeValue());
        pclOrderB2cPrintLabelsThirdDTO.setPackingList(labelType.getPackingListFlag());
        return pclOrderB2cPrintLabelsThirdDTO;
    }

    private HttpHeaders buildHttpHeadersForApi(String accessKey, String token, String apiUrl) {
        Map<String, String> authHeadersMap = WalltechSignUtil.buildHeader(HttpPost.METHOD_NAME, apiUrl, token, accessKey);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        headers.set("Accept", "application/json");
        headers.set("X-WallTech-Date", authHeadersMap.get("X-WallTech-Date"));
        headers.set("Authorization", authHeadersMap.get("Authorization"));
        return headers;
    }

    private void createB2cOrder(List<PclOrderB2cThirdDTO> pclOrderB2cDTOList, PartyVo shipper) {
        pclOrderB2cDTOList.forEach(pclOrderB2cDTO -> {
            pclOrderB2cDTO.setOrderId((BusinessNo.generator().partyId(shipper.getTenantId()).businessNoType(BusinessNoType.B2C_ORDER_NO).generate()));
            pclOrderB2cDTO.setOrderNo((BusinessNo.generator().partyId(shipper.getTenantId()).businessNoType(BusinessNoType.B2C_ORDER_NO).generate()));
            pclOrderB2cDTO.setStatus(B2cOrderStatus.UNCONFIRMED.getType());
        });
        //创建本地的订单和商品绑定信息
        createOrderAndProduct(pclOrderB2cDTOList);
    }

    private void createOrderAndProduct(List<PclOrderB2cThirdDTO> pclOrderB2cDTOList) {
        List<TrkSourceOrderEventDTO> events=new ArrayList<>();
        pclOrderB2cDTOList.forEach(pclOrderB2cThirdDTO -> {
            List<B2cOrderProductDTO> orderItems = pclOrderB2cThirdDTO.getOrderItems();
            pclOrderB2cThirdDTO.setId(IdGenerator.getInstance().generate());
            PclOrderB2c pclOrderB2c = transToPclOrderB2c(pclOrderB2cThirdDTO);
            pclOrderB2c.setServiceName(pclOrderB2cThirdDTO.getVirtualChannelName());
            pclOrderB2c.setServiceCode(pclOrderB2cThirdDTO.getVirtualChannelCode());
            pclOrderB2c.setChannelId(pclOrderB2cThirdDTO.getChannelId());
            pclOrderB2c.setShipperName(pclOrderB2cThirdDTO.getShipperNameLocal());
            pclOrderB2c.setShipperContactName(pclOrderB2cThirdDTO.getShipperName());
            pclOrderB2c.setSource(OrderSource.IMPORT.getType());
            List<PclProductB2cDTO> pclProductB2cDTOs = orderItems.stream().map(item ->{
                PclProductB2cDTO productB2cDTO = new PclProductB2cDTO();
                productB2cDTO.setSku(item.getSku());
                productB2cDTO.setProductCount(item.getItemCount() != null ? item.getItemCount() : 0);
                return productB2cDTO;
            }).collect(Collectors.toList());
            OrderPickingType pickingType = getPickingType(pclProductB2cDTOs);
            //设置拣货类型(单品单件:一个sku总数量1;单品多件:一个sku总数量大于1;多品多件:多个sku)
            pclOrderB2c.setPickingType(pickingType != null ?pickingType.getType() : null);
            //保存订单信息
            pclOrderB2cDao.insert(pclOrderB2c);
            orderEventService.packageEvents(pclOrderB2c.getOrderNo(),pclOrderB2c.getReferenceNo(),pclOrderB2c.getId(),events, WareHouseOrderEventCode.ORDER_CREATED);
            //记录服务校验切换日志
            if(pclOrderB2c.getServiceChanged()!=null&&pclOrderB2c.getServiceChanged()){
                PclOrderB2cDetailVO pclOrderB2cDetailVO = BeanUtils.transform(pclOrderB2cThirdDTO, PclOrderB2cDetailVO.class);
                pclOrderB2cDetailVO.setServiceCode(pclOrderB2cThirdDTO.getVirtualChannelCode());
                pclOrderB2cDetailVO.setServiceName(pclOrderB2cThirdDTO.getVirtualChannelName());
                recordValidationAddressLog(pclOrderB2c,pclOrderB2cDetailVO,"Upload Order");
            }
            //保存商品信息
            List<PclProductB2c> pclProductB2cList = orderItems.stream().map(orderItem -> {
                PclProductB2c pclProductB2c = new PclProductB2c();
                pclProductB2c.setOrderId(pclOrderB2c.getId());
                pclProductB2c.setSku(orderItem.getSku());
                pclProductB2c.setShipperId(pclOrderB2c.getShipperId());
                pclProductB2c.setProductCount(orderItem.getItemCount().longValue());
                pclProductB2c.setOverseasWarehouseCode(pclOrderB2c.getShipperWarehouseCode());
                //批量创建 默认商品为良品
                pclProductB2c.setInventoryType(ProductInventoryType.GOOD_PRODUCT.getCode());
                return pclProductB2c;
            }).collect(Collectors.toList());
            pclProductB2cDao.insertList(pclProductB2cList);
        });
        //创建事件
        orderEventService.addEventsByApi(events);
    }

    @Override
    public PclOrderB2c transToPclOrderB2c(PclOrderB2cThirdDTO pclOrderB2cThirdDTO) {
        PclOrderB2c pclOrderB2c = BeanUtils.transform(pclOrderB2cThirdDTO, PclOrderB2c.class);
        pclOrderB2c.setOrderNo(pclOrderB2cThirdDTO.getOrderId());
        String recipientAddressId = saveRecipientAddress(pclOrderB2cThirdDTO);
        String shipperAddressId = saveShipperAddress(pclOrderB2cThirdDTO);
        pclOrderB2c.setRecipientAddressId(recipientAddressId);
        pclOrderB2c.setShipperAddressId(shipperAddressId);
        return pclOrderB2c;
    }

    private String saveRecipientAddress(PclOrderB2cThirdDTO pclOrderB2cThirdDTO) {
        SysAddress sysAddress = BeanUtils.transform(pclOrderB2cThirdDTO, SysAddress.class);
        sysAddress.setPostCode(pclOrderB2cThirdDTO.getPostcode());
        String uuid = AddressUtils.generateId(sysAddress);
        sysAddress.setUuid(uuid);
        boolean flag = sysAddressDao.existsWithPrimaryKey(uuid);
        if (!flag) {
            sysAddress.initAddressCreateEntity();
            sysAddressDao.insertSelective(sysAddress);
        }
        return sysAddress.getUuid();
    }

    private String saveShipperAddress(PclOrderB2cThirdDTO pclOrderB2cThirdDTO) {
        SysAddress sysAddress = new SysAddress();
        sysAddress.setState(pclOrderB2cThirdDTO.getShipperState());
        sysAddress.setCountry(pclOrderB2cThirdDTO.getShipperCountry());
        sysAddress.setCity(pclOrderB2cThirdDTO.getShipperCity());
        sysAddress.setPostCode(pclOrderB2cThirdDTO.getShipperPostcode());
        sysAddress.setAddressLine1(pclOrderB2cThirdDTO.getShipperAddressLine1());
        sysAddress.setAddressLine2(pclOrderB2cThirdDTO.getShipperAddressLine2());
        sysAddress.setAddressLine3(pclOrderB2cThirdDTO.getShipperAddressLine3());
        String uuid = AddressUtils.generateId(sysAddress);
        sysAddress.setUuid(uuid);
        boolean flag = sysAddressDao.existsWithPrimaryKey(uuid);
        if (!flag) {
            sysAddress.initAddressCreateEntity();
            sysAddressDao.insertSelective(sysAddress);
        }
        return sysAddress.getUuid();
    }

    @Override
    public void checkInventory(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, PartyVo shipper, List<SubError> errors) {
        Integer aggregator = SessionContext.getContext().getAggregator();
        pclOrderB2cUploadDTOList.stream()
                .collect(Collectors
                                //按照sku和发货人仓库进行分组,和计算该商品的发货总量。
                                .groupingBy(
                                        b2cDTO -> new PclOrderB2cDTO(b2cDTO.getSkuNo(), b2cDTO.getShipperWarehouseCode()),
                                        Collectors.summarizingInt(PclOrderB2cUploadDTO::getItemCount)
                                )
                        //遍历分组后结果计算是否超过库存数
                ).forEach((k, v) -> {
                    long totalWarehouseOutCount = v.getSum();
                    //获取该sku和发货仓库的库存量
                    PclInventory pclInventory = pclInventoryDao.queryBySkuNoAndOverseasWarehouseCodeAndShipper(k.getSku(), k.getShipperWarehouseCode(), shipper.getTenantId(), aggregator);
                    if (pclInventory != null && pclInventory.getAvailableInventory() < totalWarehouseOutCount) {
                        errors.add(SubError.build(PclOrderB2cResultCode.INVENTORY_SHORTAGE, k.getSku(), k.getShipperWarehouseCode()));
                    }
                });
    }

    private void checkNotNullFields(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, List<SubError> errors) {
        pclOrderB2cUploadDTOList.forEach(pclOrderB2cUploadDTO -> {
            if (!Detect.notEmpty(pclOrderB2cUploadDTO.getReferenceNo())) {
                errors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_COLUMN_NOT_NULL, pclOrderB2cUploadDTO.getRowNum(), I18nMessageHelper.getMessage(B2cOrderUploadNeedCheckColumn.referenceNo.getMessageCode())));
            }
            if (!Detect.notEmpty(pclOrderB2cUploadDTO.getPhone()) && !Detect.notEmpty(pclOrderB2cUploadDTO.getRecipientWarehouseCode())) {
                errors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_COLUMN_NOT_NULL, pclOrderB2cUploadDTO.getRowNum(), I18nMessageHelper.getMessage(B2cOrderUploadNeedCheckColumn.phone.getMessageCode())));
            }
            if (!Detect.notEmpty(pclOrderB2cUploadDTO.getShipperWarehouseCode())) {
                errors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_COLUMN_NOT_NULL, pclOrderB2cUploadDTO.getRowNum(), I18nMessageHelper.getMessage(B2cOrderUploadNeedCheckColumn.shipperWarehouseCode.getMessageCode())));
            }
            if (!Detect.notEmpty(pclOrderB2cUploadDTO.getSkuNo())) {
                errors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_COLUMN_NOT_NULL, pclOrderB2cUploadDTO.getRowNum(), I18nMessageHelper.getMessage(B2cOrderUploadNeedCheckColumn.sku.getMessageCode())));
            }
            if (pclOrderB2cUploadDTO.getItemCount() == null || pclOrderB2cUploadDTO.getItemCount() == 0) {
                errors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_COLUMN_NOT_NULL, pclOrderB2cUploadDTO.getRowNum(), I18nMessageHelper.getMessage(B2cOrderUploadNeedCheckColumn.itemCount.getMessageCode())));
            }
            //效验交易币种和交易金额(必须同时填写或者同时不填写)
            BigDecimal transactionAmount = pclOrderB2cUploadDTO.getTransactionAmount();
            String transactionCurrency = pclOrderB2cUploadDTO.getTransactionCurrency();
            if ((transactionAmount == null && Detect.notEmpty(transactionCurrency)) ||
                    (transactionAmount != null && !Detect.notEmpty(transactionCurrency))) {
                errors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_AMOUNT_CURRENCY_ERROR, pclOrderB2cUploadDTO.getRowNum()));
            }
        });
    }

    private void checkSameReferenceNoOrder(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList,Integer shipperId, List<SubError> errors) {
        pclOrderB2cUploadDTOList.forEach(pclOrderB2cUploadDTO -> {
            List<PclOrderB2c> pclOrderB2cList = pclOrderB2cDao.findByReferenceNo(pclOrderB2cUploadDTO.getReferenceNo(), shipperId);
            if (Detect.notEmpty(pclOrderB2cList)) {
                errors.add(SubError.build(PclOrderResultCode.UPLOAD_ORDER_REF_NO_UNIQUE, pclOrderB2cUploadDTO.getRowNum(), pclOrderB2cUploadDTO.getReferenceNo()));
            }
        });
        //根据referenceNO进行分组
        Map<String, List<PclOrderB2cUploadDTO>> groupByReferenceNoMap = pclOrderB2cUploadDTOList.stream().collect(Collectors.groupingBy(PclOrderB2cUploadDTO::getReferenceNo));
        groupByReferenceNoMap.forEach((referenceNo, b2cUploadDTOList) -> {
            Map<PclOrderB2cUploadDTO, List<PclOrderB2cUploadDTO>> sameReferenceDifferentInfoMap = b2cUploadDTOList.stream()
                    .collect(Collectors
                            //将相同referenceNo的订单进行 基础信息的分组。 如果分组后Map的Size大于2 则相同referenceNo的订单有不同的其余数据 则提示报错
                            .groupingBy(
                                    b2cUploadDTO ->
                                            new PclOrderB2cUploadDTO(b2cUploadDTO.getVirtualChannelName(), b2cUploadDTO.getRecipientWarehouseCode(), b2cUploadDTO.getRecipientName(), b2cUploadDTO.getRecipientCompany(), b2cUploadDTO.getEmail(), b2cUploadDTO.getPhone()
                                                    , b2cUploadDTO.getAddressLine1(), b2cUploadDTO.getAddressLine2(), b2cUploadDTO.getAddressLine3(), b2cUploadDTO.getCity(), b2cUploadDTO.getState(), b2cUploadDTO.getPostcode()
                                                    , b2cUploadDTO.getCountry(), b2cUploadDTO.getPlatform(), b2cUploadDTO.getShipperWarehouseCode())
                            )
                    );
            if (sameReferenceDifferentInfoMap.size() > 1) {
                errors.add(SubError.build(PclOrderB2cResultCode.SAME_REFERENCE_DIFFERENT_INFO_ERROR, referenceNo));
            }
        });
    }

    @Override
    public List<PclOrderB2cThirdDTO> covertToPclOrderB2cDTO(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList, PartyVo shipper, List<SubError> errors) {
        List<PclOrderB2cThirdDTO> thirdDTOList = new ArrayList<>();
        Integer aggregator = SessionContext.getContext().getAggregator();
        if (!Detect.notEmpty(pclOrderB2cUploadDTOList)) {
            return null;
        }
        //合并商品信息一样的订单，商品数量相加
        List<PclOrderB2cUploadDTO> mergeSkuUploadDTOList=mergeSameSkuList(pclOrderB2cUploadDTOList);
        List<PclOrderB2cDTO> pclOrderB2cDTOList = mergeSkuUploadDTOList.stream().map(pclOrderB2cUploadDTO -> {
            //根据发货仓库代码和服务code查询对应的B2C的相关服务code
            String shipperWarehouseCode = pclOrderB2cUploadDTO.getShipperWarehouseCode();
            String virtualChannelName = pclOrderB2cUploadDTO.getVirtualChannelName();
            SysOverseasWarehouseShipper overseasWarehouseShipper = sysOverseasWarehouseShipperDao.getByShipperAndCode(shipper.getTenantId(), shipperWarehouseCode);
            if (overseasWarehouseShipper == null) {
                errors.add(SubError.build(PclOrderB2cResultCode.WAREHOUSE_NOT_EXIST_SHIPPER, pclOrderB2cUploadDTO.getRowNum()));
            } else {
                if (!overseasWarehouseShipper.getStatus().equals(ActiveType.ACTIVE.getType())) {
                    errors.add(SubError.build(PclOrderB2cResultCode.WAREHOUSE_NOT_ACTIVE_SHIPPER, pclOrderB2cUploadDTO.getRowNum()));
                } else {
                    SysOverseasWarehouseChannelDTO sysOverseasWarehouseChannelDTO = sysOverseasWarehouseChannelDao.selectByWarehouseAndChannelName(
                            shipperWarehouseCode, virtualChannelName, aggregator, ChannelType.FULFILMENT.getType());
                    if (sysOverseasWarehouseChannelDTO == null) {
                        errors.add(SubError.build(PclOrderB2cResultCode.CHANNEL_NOT_EXIST_WAREHOUSE, pclOrderB2cUploadDTO.getRowNum()));
                    } else {
                        if (!sysOverseasWarehouseChannelDTO.getStatus().equals(ActiveType.ACTIVE.getType())) {
                            errors.add(SubError.build(PclOrderB2cResultCode.CHANNEL_NOT_ACTIVE_WAREHOUSE, pclOrderB2cUploadDTO.getRowNum()));
                        } else {
                            List<SysOverseasWarehouseChannelSimpleDTO> activeShipperWarehouseChannels = sysOverseasWarehouseChannelDao.getByWarehouseCodeAndShipperChannelActive(shipper.getTenantId(),
                                    shipperWarehouseCode, ChannelType.FULFILMENT.getType(), ActiveTypeNew.ACTIVE.getType());
                            SysOverseasWarehouseChannelSimpleDTO targetWarehouseChannel = activeShipperWarehouseChannels.stream().filter(p -> Objects.equals(p.getId(), sysOverseasWarehouseChannelDTO.getId())).findFirst().orElse(null);
                            if (targetWarehouseChannel == null) {
                                errors.add(SubError.build(PclOrderB2cResultCode.SHIPPER_WAREHOUSE_CHANNEL_NOT_ACTIVE, pclOrderB2cUploadDTO.getRowNum()));
                            } else {
                                pclOrderB2cUploadDTO.setChannelId(sysOverseasWarehouseChannelDTO.getId());
                                if (sysOverseasWarehouseChannelDTO.getChannelCode() != null) {
                                    pclOrderB2cUploadDTO.setServiceCode(sysOverseasWarehouseChannelDTO.getChannelCode());
                                }
                                pclOrderB2cUploadDTO.setVirtualChannelCode(sysOverseasWarehouseChannelDTO.getVirtualChannelCode());
                                //上传跟踪号 只有线下服务才保存
                                if (targetWarehouseChannel != null && !targetWarehouseChannel.getIsOffline()) {
                                    pclOrderB2cUploadDTO.setTrackingNo(null);
                                }
                            }
                        }
                    }
                }
            }
            PclOrderB2cDTO pclOrderB2cDTO = BeanUtils.transform(pclOrderB2cUploadDTO, PclOrderB2cDTO.class);
            pclOrderB2cDTO.setShipperName(shipper.getNameEn());
            pclOrderB2cDTO.setWeightUnit(WeightUnit.KG.getUnit());
            pclOrderB2cDTO.setChannelId(pclOrderB2cUploadDTO.getChannelId());
            //交易币种 转成大写格式
            String transactionCurrency = pclOrderB2cUploadDTO.getTransactionCurrency();
            pclOrderB2cDTO.setTransactionCurrency(Detect.notEmpty(transactionCurrency) ? transactionCurrency.toUpperCase() : null);
            //锁定选用服务 不选默认为否
            YesOrNoType keepShipServiceYesOrNoType = YesOrNoType.getByName(pclOrderB2cUploadDTO.getKeepShipService());
            pclOrderB2cDTO.setKeepShipService(keepShipServiceYesOrNoType != null ? keepShipServiceYesOrNoType.getStatus() : false);
            perfectPclOrderB2cDTO(pclOrderB2cDTO, pclOrderB2cUploadDTO, shipper.getTenantId(), errors);
            return pclOrderB2cDTO;
        }).collect(Collectors.toList());
        if (Detect.notEmpty(errors)) {
            return thirdDTOList;
        }
        //合并相同订单号的订单
        if (Detect.notEmpty(pclOrderB2cDTOList)) {
            Map<String, List<PclOrderB2cDTO>> referenceNoMap = pclOrderB2cDTOList.stream().collect(Collectors.groupingBy(PclOrderB2cDTO::getReferenceNo));
            referenceNoMap.forEach((referenceNo, b2cDTOList) -> {
                if (b2cDTOList.size() > 0) {
                    PclOrderB2cDTO baseInfoDto = b2cDTOList.get(0);
                    PclOrderB2cThirdDTO thirdDto = BeanUtils.transform(baseInfoDto, PclOrderB2cThirdDTO.class);
                    List<B2cOrderProductDTO> b2cOrderList = b2cDTOList.stream().map(b2cDTO -> {
                        B2cOrderProductDTO b2cOrderProductDTO = new B2cOrderProductDTO();
                        b2cOrderProductDTO.setSku(b2cDTO.getSku());
                        b2cOrderProductDTO.setItemNo(b2cDTO.getSku());
                        b2cOrderProductDTO.setDescription(b2cDTO.getProductDescription());
                        b2cOrderProductDTO.setNativeDescription(b2cDTO.getProductNativeDescription());
                        b2cOrderProductDTO.setUnitValue(b2cDTO.getProductInvoiceValue());
                        b2cOrderProductDTO.setUnitValueCurrency(b2cDTO.getProductInvoiceValueCurrency());
                        b2cOrderProductDTO.setItemCount(b2cDTO.getItemCount());
                        b2cOrderProductDTO.setProductURL(b2cDTO.getProductURL());
                        b2cOrderProductDTO.setWeight(b2cDTO.getProductWeight());
                        b2cOrderProductDTO.setHsCode(b2cDTO.getHsCode());
                        b2cOrderProductDTO.setOriginCountry(b2cDTO.getShipperCountry());
                        return b2cOrderProductDTO;
                    }).collect(Collectors.toList());
                    if (b2cOrderList.size() > 0) {
                        BigDecimal totalValue = b2cOrderList.stream().map(order -> {
                            if (order.getUnitValue() == null) {
                                return BigDecimal.ZERO;
                            } else {
                                Integer productCount = order.getItemCount();
                                productCount = productCount != null ? productCount : 0;
                                return order.getUnitValue().multiply(new BigDecimal(productCount.toString()));
                            }
                        }).reduce(BigDecimal.ZERO, BigDecimal::add);
                        //处理货物描述和本地描述
                        if (!Detect.notEmpty(thirdDto.getDescription())) {
                            String productDescription = b2cOrderList.stream().map(B2cOrderProductDTO::getDescription).collect(Collectors.joining(","));
                            if(Detect.notEmpty(productDescription)){
                                if(productDescription.length()>200){
                                    thirdDto.setDescription(productDescription.substring(0,200));
                                }else{
                                    thirdDto.setDescription(productDescription);
                                }
                            }
                        }
                        if (!Detect.notEmpty(thirdDto.getNativeDescription())) {
                            String productNativeDescription = b2cOrderList.stream().map(B2cOrderProductDTO::getNativeDescription).collect(Collectors.joining(","));
                            if(Detect.notEmpty(productNativeDescription)){
                                if(productNativeDescription.length()>200){
                                    thirdDto.setNativeDescription(productNativeDescription.substring(0,200));
                                }else{
                                    thirdDto.setNativeDescription(productNativeDescription);
                                }
                            }
                        }
                        thirdDto.setShipperId(shipper.getTenantId());
                        thirdDto.setShipperName(baseInfoDto.getShipperContactName());
                        thirdDto.setShipperNameLocal(baseInfoDto.getShipperName());
                        thirdDto.setInvoiceValue(totalValue);
                        thirdDto.setInvoiceCurrency(b2cOrderList.get(0).getUnitValueCurrency());
                        thirdDto.setOrderItems(b2cOrderList);
                        thirdDto.setChannelId(baseInfoDto.getChannelId());
                    }
                    thirdDTOList.add(thirdDto);
                }
            });
        }
        return thirdDTOList;
    }

    private List<PclOrderB2cUploadDTO> mergeSameSkuList(List<PclOrderB2cUploadDTO> pclOrderB2cUploadDTOList) {
        List<PclOrderB2cUploadDTO>mergeSameSkuList=new ArrayList<>();
        //将相同referenceNo和sku的订单进行分组.
        pclOrderB2cUploadDTOList.stream()
                .collect(Collectors.groupingBy(b2cUploadDTO -> new PclOrderB2cUploadDTO(b2cUploadDTO.getReferenceNo(), b2cUploadDTO.getSkuNo())))
                .forEach((uploadDTO,sameSkuList)->{
                    PclOrderB2cUploadDTO pclOrderB2cUploadDTO = sameSkuList.get(0);
                    Integer totalItemCount = sameSkuList.stream().map(PclOrderB2cUploadDTO::getItemCount).reduce(0, Integer::sum);
                    pclOrderB2cUploadDTO.setItemCount(totalItemCount);
                    mergeSameSkuList.add(pclOrderB2cUploadDTO);
                });
        return mergeSameSkuList;
    }

    @Override
    public void perfectPclOrderB2cDTO(PclOrderB2cDTO pclOrderB2cDTO, PclOrderB2cUploadDTO pclOrderB2cUploadDTO, Integer shipperId, List<SubError> errors) {
        //完善发货人信息
        List<SubError> perfectShipperErrors = perfectShipper(pclOrderB2cDTO, pclOrderB2cUploadDTO);
        if (Detect.notEmpty(perfectShipperErrors)) {
            errors.addAll(perfectShipperErrors);
        }
        //完善收件人信息
        List<SubError> perfectRecipientErrors = perfectRecipient(pclOrderB2cDTO, pclOrderB2cUploadDTO);
        if (Detect.notEmpty(perfectRecipientErrors)) {
            errors.addAll(perfectRecipientErrors);
        }
        //通过sku 完善商品信息
        List<SubError> perfectProductErrors = perfectProductInfo(pclOrderB2cDTO, pclOrderB2cUploadDTO, shipperId, pclOrderB2cUploadDTO.getSkuNo());
        if (Detect.notEmpty(perfectProductErrors)) {
            errors.addAll(perfectProductErrors);
        }
    }

    @Override
    public List<PclOverviewDTO> overview(Integer aggregator, Integer shipperId) {
        List<Integer> shipper = pclOrderService.getSellersUnionByRoleType(SysMenuStatus.CONSIGNMENT_OF_WAREHOUSE_RECEIPTS.getType(),null );
        List<PclOverviewDTO> overviewDTOList=new ArrayList<>();
        if (!(shipper != null && shipper.size() == 0)) {
           overviewDTOList = pclOrderB2cDao.overview(aggregator, shipper);
        }
        overviewDTOList = overviewDTOList.stream().filter(p -> p.getStatus() != null).collect(Collectors.toList());
        List<PclOverviewDTO> resultList = new ArrayList<>();
        for (B2cOrderStatus value : B2cOrderStatus.values()) {
            PclOverviewDTO overviewDTO = new PclOverviewDTO();
            overviewDTO.setStatus(value.getType());
            overviewDTO.setStatusName(value.getMessage());
            PclOverviewDTO overview = overviewDTOList.stream().filter(statusCount -> statusCount.getStatus().equals(value.getType())).findFirst().orElse(null);
            if(overview!=null){
                overviewDTO.setCount(overview.getCount());
            }else{
                overviewDTO.setCount(0);
            }
            resultList.add(overviewDTO);
        }
        return resultList;
    }

    /**
     * 完善发货人信息
     *
     * @param pclOrderB2cDTO
     * @param pclOrderB2cUploadDTO
     * @return
     */
    private List<SubError> perfectShipper(PclOrderB2cDTO pclOrderB2cDTO, PclOrderB2cUploadDTO pclOrderB2cUploadDTO) {
        List<SubError> perfectShipperErrors = new ArrayList<>();
        Integer aggregator = SessionContext.getContext().getAggregator();
        String shipperWarehouseCode = pclOrderB2cUploadDTO.getShipperWarehouseCode();
        if (Detect.notEmpty(shipperWarehouseCode)) {
            SysOverseasWarehouse sysOverseasWarehouse = sysOverseasWarehouseDao.queryByCode(shipperWarehouseCode, aggregator);
            if (sysOverseasWarehouse != null) {
                pclOrderB2cDTO.setShipperContactName(sysOverseasWarehouse.getContactName());
                pclOrderB2cDTO.setShipperEmail(sysOverseasWarehouse.getContactEmail());
                pclOrderB2cDTO.setShipperPhone(sysOverseasWarehouse.getContactPhone());
                pclOrderB2cDTO.setShipperAddressLine1(sysOverseasWarehouse.getAddressLine1());
                pclOrderB2cDTO.setShipperAddressLine2(sysOverseasWarehouse.getAddressLine2());
                pclOrderB2cDTO.setShipperAddressLine3(sysOverseasWarehouse.getAddressLine3());
                pclOrderB2cDTO.setShipperCity(sysOverseasWarehouse.getCity());
                pclOrderB2cDTO.setShipperCountry(sysOverseasWarehouse.getCountry());
                pclOrderB2cDTO.setShipperState(sysOverseasWarehouse.getState());
                pclOrderB2cDTO.setShipperPostcode(sysOverseasWarehouse.getPostCode());
            } else {
                perfectShipperErrors.add(SubError.build(PclOrderB2cResultCode.SHIPPER_WAREHOUSE_CODE_NOT_NULL, pclOrderB2cUploadDTO.getRowNum()));
            }
        }
        return perfectShipperErrors;
    }

    /**
     * 完善收件人信息
     *
     * @param pclOrderB2cDTO
     * @param pclOrderB2cUploadDTO
     * @return
     */
    private List<SubError> perfectRecipient(PclOrderB2cDTO pclOrderB2cDTO, PclOrderB2cUploadDTO pclOrderB2cUploadDTO) {
        List<SubError> perfectRecipientErrors = new ArrayList<>();
        Integer aggregator = SessionContext.getContext().getAggregator();
        String recipientWarehouseCode = pclOrderB2cUploadDTO.getRecipientWarehouseCode();
        if (Detect.notEmpty(recipientWarehouseCode)) {
            SysWarehouse sysWarehouse = sysWarehouseDao.findByCode(recipientWarehouseCode, aggregator, null);
            if (sysWarehouse != null) {
                pclOrderB2cDTO.setRecipientName(sysWarehouse.getContactName());
                pclOrderB2cDTO.setRecipientCompany(sysWarehouse.getCompanyName());
                pclOrderB2cDTO.setPhone(sysWarehouse.getContactPhone());
                pclOrderB2cDTO.setEmail(sysWarehouse.getContactEmail());
                pclOrderB2cDTO.setAddressLine1(sysWarehouse.getAddressLine1());
                pclOrderB2cDTO.setAddressLine2(sysWarehouse.getAddressLine2());
                pclOrderB2cDTO.setAddressLine3(sysWarehouse.getAddressLine3());
                pclOrderB2cDTO.setCity(sysWarehouse.getCity());
                pclOrderB2cDTO.setCountry(sysWarehouse.getCountry());
                pclOrderB2cDTO.setState(sysWarehouse.getState());
                pclOrderB2cDTO.setPostcode(sysWarehouse.getPostCode());
            }
        }
        return perfectRecipientErrors;
    }

    /**
     * 完善收件人信息
     *
     * @param pclOrderB2cUploadDTO
     * @return
     */
    private List<SubError> perfectRecipient(PclOrderB2cUploadDTO pclOrderB2cUploadDTO) {
        List<SubError> perfectRecipientErrors = new ArrayList<>();
        Integer aggregator = SessionContext.getContext().getAggregator();
        String recipientWarehouseCode = pclOrderB2cUploadDTO.getRecipientWarehouseCode();
        log.info("收件人地址代码为:{}", recipientWarehouseCode);
        if (Detect.notEmpty(recipientWarehouseCode)) {
            SysWarehouse sysWarehouse = sysWarehouseDao.findByCode(recipientWarehouseCode, aggregator, null);
            if (sysWarehouse != null) {
                pclOrderB2cUploadDTO.setRecipientName(sysWarehouse.getContactName());
                pclOrderB2cUploadDTO.setRecipientCompany(sysWarehouse.getCompanyName());
                pclOrderB2cUploadDTO.setPhone(sysWarehouse.getContactPhone());
                pclOrderB2cUploadDTO.setEmail(sysWarehouse.getContactEmail());
                pclOrderB2cUploadDTO.setAddressLine1(sysWarehouse.getAddressLine1());
                pclOrderB2cUploadDTO.setAddressLine2(sysWarehouse.getAddressLine2());
                pclOrderB2cUploadDTO.setAddressLine3(sysWarehouse.getAddressLine3());
                pclOrderB2cUploadDTO.setCity(sysWarehouse.getCity());
                pclOrderB2cUploadDTO.setCountry(sysWarehouse.getCountry());
                pclOrderB2cUploadDTO.setState(sysWarehouse.getState());
                pclOrderB2cUploadDTO.setPostcode(sysWarehouse.getPostCode());
            }
        }
        return perfectRecipientErrors;
    }

    private List<SubError> checkAddress(PclOrderB2cUploadDTO pclOrderB2cUploadDTO) {
        List<SubError> addressErrors = new ArrayList<>();
        if (pclOrderB2cUploadDTO.getRecipientName() == null) {
            addressErrors.add(SubError.build(PclOrderB2cResultCode.CONSIGNEE_NAME_NOT_NULL, pclOrderB2cUploadDTO.getRowNum()));
        }
        if (pclOrderB2cUploadDTO.getCountry() == null) {
            addressErrors.add(SubError.build(PclOrderB2cResultCode.CONSIGNEE_COUNTRY_CODE_NOT_NULL, pclOrderB2cUploadDTO.getRowNum()));
        }
        if (pclOrderB2cUploadDTO.getCity() == null) {
            addressErrors.add(SubError.build(PclOrderB2cResultCode.CONSIGNEE_CITY_NOT_NULL, pclOrderB2cUploadDTO.getRowNum()));
        }
        if (pclOrderB2cUploadDTO.getAddressLine1() == null) {
            addressErrors.add(SubError.build(PclOrderB2cResultCode.CONSIGNEE_ADDRESS1_NOT_NULL, pclOrderB2cUploadDTO.getRowNum()));
        }
        if (pclOrderB2cUploadDTO.getPostcode() == null) {
            addressErrors.add(SubError.build(PclOrderB2cResultCode.POST_NOT_NULL, pclOrderB2cUploadDTO.getRowNum()));
        }
        return addressErrors;
    }


    /**
     * 获取库存中信息
     *
     * @param pclOrderB2cDTO
     * @param pclOrderB2cUploadDTO
     * @param shipperId
     * @param skuNo
     * @return
     */
    private List<SubError> perfectProductInfo(PclOrderB2cDTO pclOrderB2cDTO, PclOrderB2cUploadDTO pclOrderB2cUploadDTO, Integer shipperId, String skuNo) {
        List<SubError> perfectProductErrors = new ArrayList<>();
        Integer aggregator = SessionContext.getContext().getAggregator();
        Integer rowNumber = pclOrderB2cUploadDTO.getRowNum();
        //获取该仓库库存中是否存在该商品
        PclInventory pclInventory = pclInventoryDao.queryBySkuNoAndOverseasWarehouseCodeAndShipper(skuNo, pclOrderB2cDTO.getShipperWarehouseCode(), shipperId, aggregator);
        if (pclInventory == null) {
            perfectProductErrors.add(SubError.build(PclOrderB2cResultCode.WAREHOUSE_SKU_NOT_EXIST, rowNumber));
        } else {
            PclTransportOrderPackingDetailVO sysProduct = sysProductDao.queryBySkuAndShipperForTransportPackingDetail(skuNo, shipperId, pclOrderB2cDTO.getShipperWarehouseCode(), ProductType.OVERSEASWAREHOUSE.getType());
            if (sysProduct == null) {
                //发货的卖家 没有该商品库信息
                perfectProductErrors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_SKU_NOT_EXIST, rowNumber));
            } else {
                if (StatusType.INVALID.getStatus().equals(sysProduct.getStatus())) {
                    //发货的卖家 该商品库信息 被禁用了
                    perfectProductErrors.add(SubError.build(PclOrderB2cResultCode.ORDER_UPLOAD_SKU_BANDED, rowNumber));
                } else {
                    //查询该商品对应目的国的hsCode
                    if (Detect.notEmpty(pclOrderB2cDTO.getCountry())) {
                        SysProductDTO productWithHsCode = sysProductDao.findByShipperAndCountryCodeAndSku(shipperId, pclOrderB2cDTO.getCountry(), skuNo, ProductType.OVERSEASWAREHOUSE.getType());
                        if (productWithHsCode != null) {
                            sysProduct.setHsCode(productWithHsCode.getHsCode());
                        }
                    }
                }
                if(sysProduct.getWarehouseProductStatus().equals(ActiveType.INVALID.getType())){
                    perfectProductErrors.add(SubError.build(PclBoxResultCode.SKU_DISABLED_IN_WAREHOUSE_ERROR, rowNumber,skuNo));
                }
                //完善订单的商品信息
                pclOrderB2cDTO.setSku(sysProduct.getSku());
                pclOrderB2cDTO.setItemNo(sysProduct.getSku());
                pclOrderB2cDTO.setProductDescription(sysProduct.getSkuNameEn());
                pclOrderB2cDTO.setProductNativeDescription(sysProduct.getSkuNameCn());
                pclOrderB2cDTO.setProductInvoiceValue(sysProduct.getInvoiceValue());
                pclOrderB2cDTO.setProductInvoiceValueCurrency(sysProduct.getCurrency());
                pclOrderB2cDTO.setProductURL(sysProduct.getUrl());
                pclOrderB2cDTO.setProductWeight(sysProduct.getGrossWeightActual());
                pclOrderB2cUploadDTO.setProductHeight(sysProduct.getProductHeightActual());
                pclOrderB2cUploadDTO.setProductLength(sysProduct.getProductLengthActual());
                pclOrderB2cUploadDTO.setProductWidth(sysProduct.getProductWidthActual());
                pclOrderB2cUploadDTO.setWeight(sysProduct.getGrossWeightActual());
                pclOrderB2cUploadDTO.setWeightUnit(sysProduct.getWeightUnit());
                pclOrderB2cUploadDTO.setDimensionUnit(sysProduct.getDimensionUnit());
            }
        }
        return perfectProductErrors;
    }

    @Override
    public PclOrderB2cForLogVO selectById(Long id) {
        if (id == null) {
            return new PclOrderB2cForLogVO();
        }
        PclOrderB2cForLogDTO pclOrderB2cForLogDTO= pclOrderB2cDao.selectPclOrderB2cBaseInfoForLog(id);
        if (pclOrderB2cForLogDTO == null) {
            return null;
        }
        List<PclOrderB2cAttachment> attachments = pclOrderB2cAttachmentDao.selectByOrderId(id);
        pclOrderB2cForLogDTO.setFileList(BeanUtils.transform(attachments, AttachmentForLogVO.class));
        if(pclOrderB2cForLogDTO.getStatus()!=null){
            B2cOrderStatus b2cOrderStatus = B2cOrderStatus.get(pclOrderB2cForLogDTO.getStatus());
            if(b2cOrderStatus!=null){
                pclOrderB2cForLogDTO.setStatusMsg(b2cOrderStatus.getMessage());
            }
        }
        if(pclOrderB2cForLogDTO.getPickingType()!=null){
            OrderPickingType orderPickingType = OrderPickingType.get(pclOrderB2cForLogDTO.getPickingType());
            if(orderPickingType!=null){
                pclOrderB2cForLogDTO.setPickingTypeMsg(orderPickingType.getMessage());
            }
        }
        if(pclOrderB2cForLogDTO.getServiceName()!=null&&pclOrderB2cForLogDTO.getServiceCode()!=null){
            pclOrderB2cForLogDTO.setServiceName("("+pclOrderB2cForLogDTO.getServiceCode()+")"+pclOrderB2cForLogDTO.getServiceName());
        }
        Integer shipperId = pclOrderB2cForLogDTO.getShipperId();
        SysParty shipper = sysPartyService.getByPartyId(shipperId);
        if(shipper!=null){
            pclOrderB2cForLogDTO.setShipperName("("+shipper.getTenantId()+")"+shipper.getNameLocal());
        }
        PclOrderB2cForLogVO pclOrderB2cForLogVO = BeanUtils.transform(pclOrderB2cForLogDTO, PclOrderB2cForLogVO.class);
        List<PclProductB2c> productB2cList = pclProductB2cDao.findByOrderId(id);
        List<OperatorLogExtraInfoDTO>productInfo=productB2cList.stream().map(product->{
            OperatorLogExtraInfoDTO operatorLogExtraInfoDTO=new OperatorLogExtraInfoDTO();
            operatorLogExtraInfoDTO.setId(product.getId());
            operatorLogExtraInfoDTO.setContent(product.getSku()+"*"+product.getProductCount());
            return operatorLogExtraInfoDTO;
        }).collect(Collectors.toList());
        pclOrderB2cForLogVO.setProductInfo(productInfo);
        return pclOrderB2cForLogVO;
    }

    @Override
    public List<PclOrderB2cForLogVO> selectByIdList(List<Long> idList) {
        List<PclOrderB2cForLogVO>resultList=new ArrayList<>();
        List<PclOrderB2cForLogDTO> pclOrderB2cForLogDTOList= pclOrderB2cDao.selectPclOrderB2cBaseInfoForLogByIdList(idList);
        pclOrderB2cForLogDTOList.forEach(pclOrderB2cForLogDTO -> {
            if(pclOrderB2cForLogDTO.getStatus()!=null){
                B2cOrderStatus b2cOrderStatus = B2cOrderStatus.get(pclOrderB2cForLogDTO.getStatus());
                if(b2cOrderStatus!=null){
                    pclOrderB2cForLogDTO.setStatusMsg(I18nMessageHelper.getMessage(b2cOrderStatus.getMessageKey(), Locale.SIMPLIFIED_CHINESE, null));
                }
            }
            if(pclOrderB2cForLogDTO.getPickingType()!=null){
                OrderPickingType orderPickingType = OrderPickingType.get(pclOrderB2cForLogDTO.getPickingType());
                if(orderPickingType!=null){
                    pclOrderB2cForLogDTO.setPickingTypeMsg(I18nMessageHelper.getMessage(orderPickingType.getMessageKey(), Locale.SIMPLIFIED_CHINESE, null));
                }
            }
            if(pclOrderB2cForLogDTO.getServiceName()!=null&&pclOrderB2cForLogDTO.getServiceCode()!=null){
                pclOrderB2cForLogDTO.setServiceName("("+pclOrderB2cForLogDTO.getServiceCode()+")"+pclOrderB2cForLogDTO.getServiceName());
            }
            Integer shipperId = pclOrderB2cForLogDTO.getShipperId();
            SysParty shipper = sysPartyService.getByPartyId(shipperId);
            if(shipper!=null){
                pclOrderB2cForLogDTO.setShipperName("("+shipper.getTenantId()+")"+shipper.getNameLocal());
            }
            PclOrderB2cForLogVO pclOrderB2cForLogVO = BeanUtils.transform(pclOrderB2cForLogDTO, PclOrderB2cForLogVO.class);
            if (pclOrderB2cForLogDTO.getStatus() != null) {
                B2cOrderStatus b2cOrderStatus = B2cOrderStatus.get(pclOrderB2cForLogDTO.getStatus());
                if (b2cOrderStatus != null) {
                    pclOrderB2cForLogVO.setStatus18nKey(b2cOrderStatus.getMessageKey());
                }
            }

            List<PclProductB2c> productB2cList = pclProductB2cDao.findByOrderId(pclOrderB2cForLogDTO.getId());
            List<OperatorLogExtraInfoDTO>productInfo=productB2cList.stream().map(product->{
                OperatorLogExtraInfoDTO operatorLogExtraInfoDTO=new OperatorLogExtraInfoDTO();
                operatorLogExtraInfoDTO.setId(product.getId());
                operatorLogExtraInfoDTO.setContent(product.getSku()+"*"+product.getProductCount());
                return operatorLogExtraInfoDTO;
            }).collect(Collectors.toList());
            pclOrderB2cForLogVO.setProductInfo(productInfo);
            resultList.add(pclOrderB2cForLogVO);
        });
        return resultList;
    }

    private List<SysProductCheckDTO> convertUploadDtoToProductDTO(List<PclOrderB2cUploadDTO> uploadDTOS){
        if(CollectionUtils.isEmpty(uploadDTOS)){
            return new ArrayList<>();
        }
        List<SysProductCheckDTO> productCheckDTOS = new ArrayList<>(uploadDTOS.size());
        uploadDTOS.forEach(e ->{
            SysProductCheckDTO checkDTO = new SysProductCheckDTO();
            checkDTO.setProductLengthActual(e.getProductLength());
            checkDTO.setProductHeightActual(e.getProductHeight());
            checkDTO.setProductWidthActual(e.getProductWidth());
            checkDTO.setGrossWeightActual(e.getWeight());
            checkDTO.setGoodsQty(e.getItemCount());
            checkDTO.setWeightUnit(e.getWeightUnit());
            checkDTO.setDimensionUnit(e.getDimensionUnit());
            productCheckDTOS.add(checkDTO);
        });
        return productCheckDTOS;
    }

    public void addEvent(){

    }

    public BigDecimal getCustomFee(CostCalculationDTO costCalculationDTO, Long remoteAreaSurchargeId, BigDecimal weight, List<CostCalculationProductDTO> pclOrderB2cOrderDTO) {
        log.info("计算自定义附加费Start");
        if (remoteAreaSurchargeId == null) {
            log.info("当前附加费未配自定义附加费,无法计算自定义附加费");
            return null;
        }
        //获取再有效期内的选项

        ActLastMileCustomSurchargeDetailDTO item = actLastMileCustomSurchargeService.getItemList(remoteAreaSurchargeId);
        if (Objects.isNull(item)) {
            return null;
        }
        List<ActLastMileCustomItemDTO> lastMileDetails = item.getLastMileDetails();
        List<ActLastMileCustomRuleSettingsDTO> ruleSettingsList = item.getRuleSettingsList();

        CustomSurchargeBillingType typeEnum = CustomSurchargeBillingType.getByType(item.getType());
        log.info("计算自定义附加费枚举:{}", typeEnum.getMessage());


        Date now = new Date();
        log.info("当前时间:{}", DateUtils.format(now, DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1));

        BigDecimal calculateFinalAmount = BigDecimal.ZERO;
        switch (typeEnum.getType()) {
            case 1:
                //按重量计费
                Integer scopeApplication = item.getScopeApplication().intValue();
                //匹配类型 0 全部 1.任一规则
                Short matchType = item.getMatchType();
                Short itemCarryRule = item.getCarryRule();
                //匹配规则
                boolean isCheck = rulesVerification(ruleSettingsList, pclOrderB2cOrderDTO, weight, matchType, scopeApplication);
                if (!isCheck) {
                    log.info("未匹配到规则,无法计算自定义附加费");
                    return null;
                }
                //1.匹配有效期，2.匹配费用分区  3.匹配重量段区间
                ActLastMileCustomItemDetailDTO selectedItem = null;
                for (ActLastMileCustomItemDTO lastMileDetail : lastMileDetails) {
                    //匹配有效期
                    Date validityStart = lastMileDetail.getValidityStart();
                    Date validityEnd = lastMileDetail.getValidityEnd();
                    if (validityStart != null) {
                        if (validityStart.compareTo(now) > 0) {
                            continue;
                        }
                    }
                    if (validityEnd != null) {
                        if (validityEnd.compareTo(now) <= 0) {
                            continue;
                        }
                    }
                    log.info("匹配上的有效期区间为:{} - {}", DateUtils.format(validityStart, DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1), DateUtils.format(validityEnd, DateUtils.DATE_TIME_FORMAT_YYYY_MM_DD_HH_MI_SS1));
                    List<ActLastMileCustomItemDetailDTO> detailCreateList = lastMileDetail.getDetailCreateList();
                    for (ActLastMileCustomItemDetailDTO actLastMileCustomItemDetailDTO : detailCreateList) {
                        Integer weights = checkCountryAndZoneItemInfo(costCalculationDTO, actLastMileCustomItemDetailDTO.getCountryCode(), actLastMileCustomItemDetailDTO.getZoneItemId());
                        if (weights == -1) {
                            continue;
                        }
                        //匹配重量段区间
                        //匹配重量段
                        BigDecimal weightStart = actLastMileCustomItemDetailDTO.getWeightStart();
                        BigDecimal weightEnd = actLastMileCustomItemDetailDTO.getWeightEnd();
                        if (weightStart != null && BigDecimalUtils.le(weight, weightStart)) {
                            continue;
                        }
                        if (weightEnd != null && BigDecimalUtils.gt(weight, weightEnd)) {
                            continue;
                        }
                        selectedItem = actLastMileCustomItemDetailDTO;

                        log.info("匹配上的价卡区间为:{} - {}", weightStart, weightEnd);
                        break;
                    }
                }
                //匹配匹配
                if (selectedItem != null) {
                    calculateFinalAmount = calculateFinalAmount(selectedItem, itemCarryRule.intValue(),weight);
                }
                break;
            case 2:
                //按单价计费
                break;
            case 3:
                //按比例计费
                break;
        }
        return null;
    }

    public boolean rulesVerification(List<ActLastMileCustomRuleSettingsDTO> ruleSettingsList, List<CostCalculationProductDTO> pclOrderB2cOrderDTO, BigDecimal weight, Short matchType, Integer scopeApplication) {
        boolean isCheck = false;
        //1.适用范围 2.需要匹配规则
        switch (scopeApplication) {
            case 1:
                //适用范围
                isCheck = true;
                break;
            case 2:
                //需要匹配规则
                for (ActLastMileCustomRuleSettingsDTO actLastMileCustomRuleSettingsDTO : ruleSettingsList) {
                    boolean rule = checkRules(actLastMileCustomRuleSettingsDTO, pclOrderB2cOrderDTO, weight);
                    if (matchType == 0) {
                        if (!rule) {
                            break;
                        }
                    } else {
                        if (rule) {
                            isCheck = true;
                            break;
                        }
                    }
                }
                break;
        }
        return isCheck;
    }

    public boolean checkRules(ActLastMileCustomRuleSettingsDTO actLast, List<CostCalculationProductDTO> products, BigDecimal weight) {
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(products) || products.size() > 1) {
            return false;
        }
        // 设置运算符和规则值
        LastMileTaxMatchingRulesParametersType parameter = LastMileTaxMatchingRulesParametersType.getByCode(actLast.getRuleParam());
        if (parameter == null) {
            return false;
        }
        LastMileTaxMatchingRulesValuesType operator = LastMileTaxMatchingRulesValuesType.getByCode(actLast.getRuleValue());
        BigDecimal ruleValueOpen = null;  // 参数值
        boolean isWithinRule = false;
        for (CostCalculationProductDTO product : products) {
            BigDecimal productLengthActual = product.getProductLengthActual() != null ? product.getProductLengthActual() : BigDecimal.ZERO;
            BigDecimal productWidthActual = product.getProductWidthActual() != null ? product.getProductWidthActual() : BigDecimal.ZERO;
            BigDecimal productHeightActual = product.getProductHeightActual() != null ? product.getProductHeightActual() : BigDecimal.ZERO;
            // 根据parameter选择不同的参数值
            switch (parameter) {
                case LONGEST_SIDE:
                    // 设置最长边的规则值
                    ruleValueOpen = productLengthActual.max(productWidthActual).max(productHeightActual);
                    break;
                case SHORTEST_SIDE:
                    // 设置最短边的规则值
                    ruleValueOpen = productLengthActual.min(productWidthActual).min(productHeightActual);
                    break;
                case SECOND_LONGEST_SIDE:
                    // 设置次长边的规则值
                    BigDecimal[] dimensions = {productLengthActual, productWidthActual, productHeightActual};
                    Arrays.sort(dimensions);
                    ruleValueOpen = dimensions[1];
                    break;
                case CHARGE_WEIGHT:
                    // 设置重量的规则值 计费重量
                    ruleValueOpen = weight;
                    break;
                case L2W_H:
                    // 设置长度宽度比的规则值 L+2(W+H)
                    BigDecimal l2w_h = productLengthActual.add(productWidthActual.multiply(new BigDecimal(2))
                            .add(productHeightActual.multiply(new BigDecimal(2))));
                    ruleValueOpen = l2w_h;
                    break;
                case PIECES:
                    // 设置数量的规则值 件数
                    ruleValueOpen = new BigDecimal(products.size());
                    break;
                default:
                    System.out.println("Invalid parameter");
                    break;
            }
            if (ruleValueOpen == null) {
                return false;
            }
            // 要判断的值
            BigDecimal value = new BigDecimal("15");

            // 判断是否在规则内
            if (operator.getType() == 1) { // 固定值
                switch (operator) {
                    case fixedValue_MatchingRules_less_than: // 小于
                        isWithinRule = value.compareTo(ruleValueOpen) < 0;
                        break;
                    case fixedValue_MatchingRules_less_than_or_equal: // 小于等于
                        isWithinRule = value.compareTo(ruleValueOpen) <= 0;
                        break;
                    case fixedValue_MatchingRules_greater_than: // 大于
                        isWithinRule = value.compareTo(ruleValueOpen) > 0;
                        break;
                    case fixedValue_MatchingRules_greater_than_or_equal: // 大于等于
                        isWithinRule = value.compareTo(ruleValueOpen) >= 0;
                        break;
                    case fixedValue_MatchingRules_equal: // 等于
                        isWithinRule = value.compareTo(ruleValueOpen) == 0;
                        break;
                    case fixedValue_MatchingRules_not_equal: // 不等于
                        isWithinRule = value.compareTo(ruleValueOpen) != 0;
                        break;
                }
            } else if (operator.getType() == 2) { // 范围值
                BigDecimal ruleValueClose = new BigDecimal("20");  // 设置规则的结束值
                switch (operator) {
                    case range_MatchingRules_open_open: // 开区间-开区间
                        isWithinRule = value.compareTo(ruleValueOpen) > 0 && value.compareTo(ruleValueClose) < 0;
                        break;
                    case range_MatchingRules_close_close: // 闭区间-闭区间
                        isWithinRule = value.compareTo(ruleValueOpen) >= 0 && value.compareTo(ruleValueClose) <= 0;
                        break;
                    case range_MatchingRules_open_close: // 开区间-闭区间
                        isWithinRule = value.compareTo(ruleValueOpen) > 0 && value.compareTo(ruleValueClose) <= 0;
                        break;
                    case range_MatchingRules_close_open: // 闭区间-开区间
                        isWithinRule = value.compareTo(ruleValueOpen) >= 0 && value.compareTo(ruleValueClose) < 0;
                        break;
                }
            }
        }
        return isWithinRule;
    }

    public static BigDecimal calculateFinalAmount(ActLastMileCustomItemDetailDTO selectedItem, Integer carryRule, BigDecimal chargeWeight) {
        BigDecimal finalAmount;
        selectedItem.setContinuousWeightUnitWeight(new BigDecimal(5));
        // 首重
        BigDecimal firstWeight = selectedItem.getFirstWeight() != null ? selectedItem.getFirstWeight() : BigDecimal.ZERO;

        // 首重价格
        BigDecimal firstWeightPrice = selectedItem.getFirstWeightPrice() != null ? selectedItem.getFirstWeightPrice() : BigDecimal.ZERO;

        // 续重单位重量
        BigDecimal additionalUnitWeight = selectedItem.getContinuousWeightUnitWeight() != null ? selectedItem.getContinuousWeightUnitWeight() : BigDecimal.ONE;

        // 续重单价
        BigDecimal unitPrice = selectedItem.getAdditionalWeightUnitPrice() != null ? selectedItem.getAdditionalWeightUnitPrice() : BigDecimal.ZERO;

        // 最低收费
        BigDecimal minimumCharge = selectedItem.getMinimumCharge() != null ? selectedItem.getMinimumCharge() : BigDecimal.ZERO;

        // 最高收费
        BigDecimal maximumCharge = selectedItem.getMaximumCharge() != null ? selectedItem.getMaximumCharge() : BigDecimal.ZERO;

        // 计费数量 = (计费重量 - 首重) / 续重单位重量
        BigDecimal quantity;
        if (additionalUnitWeight.compareTo(BigDecimal.ZERO) == 0) {
            quantity = BigDecimal.ZERO;
        } else {
            quantity = chargeWeight.subtract(firstWeight).divide(additionalUnitWeight, 3, getCarryRule(carryRule)).max(BigDecimal.ZERO);
        }
        // 计算金额 = (计费数量 * 续重单价 + 首重价格)
        BigDecimal calcAmount = unitPrice.multiply(quantity).add(firstWeightPrice);

        finalAmount = calcAmount;
        //小于最小取最小
        if(Objects.nonNull(minimumCharge) && !minimumCharge.equals(BigDecimal.ZERO)){
            if (calcAmount.compareTo(minimumCharge) < 0){
                finalAmount = minimumCharge;
            }
        }
        //大于最大取最大
        if(Objects.nonNull(maximumCharge) && !maximumCharge.equals(BigDecimal.ZERO)){
            if (calcAmount.compareTo(maximumCharge) > 0){
                finalAmount = maximumCharge;
            }
        }

        return finalAmount;
    }


    /**
     * 获取进位规则
     *
     * @param carryRule 进位规则，1 表示向上取整，其他表示不进位
     * @return 进位规则常量
     */
    private static int getCarryRule(int carryRule) {
        return carryRule == 1 ? BigDecimal.ROUND_UP : BigDecimal.ROUND_DOWN;
    }
}