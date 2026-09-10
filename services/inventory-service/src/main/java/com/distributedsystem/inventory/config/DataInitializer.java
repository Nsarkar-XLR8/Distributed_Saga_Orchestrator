package com.distributedsystem.inventory.config;

import com.distributedsystem.inventory.entity.Product;
import com.distributedsystem.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;

    @Override
    public void run(String... args) {
        if (productRepository.count() == 0) {
            log.info("Seeding initial products into inventory database...");
            List<Product> seedProducts = List.of(
                Product.builder().id("prod_macbook").sku("SKU-MACBOOK-M3").name("Apple MacBook Pro 16 M3").totalStock(10).reservedStock(0).build(),
                Product.builder().id("prod_iphone").sku("SKU-IPHONE-15").name("Apple iPhone 15 Pro").totalStock(25).reservedStock(0).build(),
                Product.builder().id("prod_sony_headphones").sku("SKU-SONY-WH1000XM5").name("Sony WH-1000XM5 ANC Headphones").totalStock(50).reservedStock(0).build(),
                Product.builder().id("prod_dell_monitor").sku("SKU-DELL-U2723QE").name("Dell UltraSharp 27 4K Monitor").totalStock(15).reservedStock(0).build(),
                Product.builder().id("prod_logitech_mouse").sku("SKU-LOGI-MX3S").name("Logitech MX Master 3S").totalStock(100).reservedStock(0).build()
            );
            for (Product p : seedProducts) {
                if (p != null) {
                    productRepository.save(p);
                }
            }
            log.info("Successfully seeded {} products.", seedProducts.size());
        }
    }
}
