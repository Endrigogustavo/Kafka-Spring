package com.integracao.kafka.adapter.in.http;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class HomeController {

    @GetMapping("/")
    public String home() {
        return "redirect:/swagger-ui.html";
    }

    @GetMapping("/swagger")
    public String swagger() {
        return "redirect:/swagger-ui.html";
    }

    @GetMapping("/docs")
    public String docs() {
        return "redirect:/swagger-ui.html";
    }
}
