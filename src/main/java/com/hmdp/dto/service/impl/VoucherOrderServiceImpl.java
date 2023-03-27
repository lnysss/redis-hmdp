package com.hmdp.dto.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.dto.service.ISeckillVoucherService;
import com.hmdp.dto.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1204*1024);

    private static final ExecutorService seckill_order_executor = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        seckill_order_executor.submit(new voucherOrderHandler());
    }

    private class voucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while(true){
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.order", ReadOffset.lastConsumed())
                    );
                    if(list == null || list.isEmpty()){
                        continue;
                    }
                    MapRecord<String, Object, Object> redord = list.get(0);
                    Map<Object, Object> values = redord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge("stream.order","g1",redord.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.order", ReadOffset.from("0"))
                    );
                    if(list == null || list.isEmpty()){
                        break;
                    }
                    MapRecord<String, Object, Object> redord = list.get(0);
                    Map<Object, Object> values = redord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    stringRedisTemplate.opsForStream().acknowledge("stream.order","g1",redord.getId());
                } catch (Exception e) {
                    log.error("处理pendinglist异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }

       /* @Override
        public void run() {
            while(true){
                try {
                    VoucherOrder voucherorder = orderTasks.take();
                    handleVoucherOrder(voucherorder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }*/
    }

    private void handleVoucherOrder(VoucherOrder voucherorder) {

        Long userId = voucherorder.getUserId();
        RLock lock = redissonClient.getLock("lock:order" + userId);

        boolean isLock = lock.tryLock();

        if(!isLock){
            log.error("不允许重复下单");
            return;
        }

        try{
            proxy.createVoucherOrder(voucherorder);
        }finally {
            lock.unlock();
        }
    }


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId));

        int r = result.intValue();
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        proxy = (IVoucherOrderService)AopContext.currentProxy();

        return Result.ok(orderId);
    }
    /*@Override
    public Result seckillVoucher(Long voucherId) {

        Long userId = UserHolder.getUser().getId();

        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString());

        int r = result.intValue();
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        VoucherOrder voucherOrder = new VoucherOrder();

        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setVoucherId(orderId);

        voucherOrder.setUserId(userId);

        voucherOrder.setVoucherId(voucherId);

        orderTasks.add(voucherOrder);

        proxy = (IVoucherOrderService)AopContext.currentProxy();

        return Result.ok(orderId);
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {

        Long userid = UserHolder.getUser().getId();

        RLock redislock = redissonClient.getLock("lock:order" + userid);

        boolean isLock = redislock.tryLock();

        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }


        int count = query().eq("user_id", userid).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }

        boolean success = seckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();

        if (!success) {
            log.error("库存不足");
            return;
        }

        save(voucherOrder);

    }


  // @Override
  // public Result seckillVoucher(Long voucherId) {

  //     SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

  //     if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
  //         return Result.fail("未开始");
  //     }

  //     if(voucher.getBeginTime().isBefore(LocalDateTime.now())){
  //         return Result.fail("已结束");
  //     }

  //     if(voucher.getStock() < 1){
  //         return Result.fail("库存不足");
  //     }
  //     Long userid = UserHolder.getUser().getId();

  //     RLock lock = redissonClient.getLock("lock:order" + userid);

  //     boolean isLock = lock.tryLock();

  //     if(!isLock){
  //         return Result.fail("不能重复下单");
  //     }

  //     try{
  //         IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
  //         return proxy.createVoucherOrder(voucherId);
  //     }finally {
  //         lock.unlock();
  //     }
  // }
}
